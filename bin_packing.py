
import pyspark.sql.functions as func

from pyspark.sql.types import StringType


from operator import itemgetter
import traceback
from pyspark.sql import Row

BIN_ID = 'join_id'
GROUP_ID = 'bin_packing'


def add_join_key(src_df, col_names_to_concat, concat_col_name):
    """
    Adding to it new column that is concat of number of columns.
    This method is optional, if the join is base on only one column, no need to use this method .
    :param src_df: return this dataframe with the new column
    :param col_names_to_concat: array of columns names
    :param concat_col_name: the new column name.
    :return: the src df with new column
    """
    concat_udf = func.udf(lambda cols: "_".join([str(x) if x is not None else "*" for x in cols]), StringType())
    return src_df.withColumn(concat_col_name, concat_udf(
        func.array(col_names_to_concat)))


def bin_packing(weights_list, max_weight_in_bin=None):
    """
    :param weights_list: python list contains 'weight' for each key (or keys). weight represents the count of each key.
    :param max_weight_in_bin: must be equal or bigger than the max weight in the weights_list. If None, using the max weight in list
    :return:
    """
    bin_id = 1
    weights_list = sorted(weights_list, key=itemgetter('weight'), reverse=True)
    if max_weight_in_bin is None:
        max_weight_in_bin = weights_list[0]['weight']

    for index, item in enumerate(weights_list):
        if item.get(BIN_ID) is None:
            sum_of_weight = 0
            for item_inner in weights_list[index:]:
                if (item_inner.get(BIN_ID) is None) and (item_inner['weight'] + sum_of_weight <= max_weight_in_bin):
                    item_inner[BIN_ID] = bin_id
                    sum_of_weight += item_inner['weight']

            bin_id += 1

    return weights_list


def print_query(query_string):
    """
    :param query_string:
    :return:
    """
    print "*******************************************************************************************"
    print "Run Query:"
    print query_string
    print "*******************************************************************************************"


def add_join_id_column(spark_session, df_src, df_weights, src_col_name, weights_col_name):
    """
    :param spark_session: the spark session
    :param df_src: spark datafarme which need to be updated with join_id
    :param df_weights: spark datafarme with weights_col_name and 'join_id'
    :param src_col_name:
    :param weights_col_name:
    :return: spark dataframe
    """

    src_table_name = 'src_table'
    weights_table_name = 'weights_table'
    df_src.registerTempTable(src_table_name)
    df_weights.registerTempTable(weights_table_name)
    selected_columns = ['l' + "." + col_name for col_name in df_src.columns] + ['r.' + BIN_ID]

    query_add_join_id = '''SELECT %s 
                               FROM %s as l 
                               LEFT OUTER JOIN (SELECT /*+ BROADCAST (%s) */ * from (%s)) as r 
                               ON l.%s = r.%s''' % (', '.join(selected_columns), src_table_name, weights_table_name, weights_table_name, src_col_name, weights_col_name)

    print_query(query_add_join_id)

    df_left_with_join_id=spark_session.sql(query_add_join_id)
    spark_session.catalog.dropTempView(src_table_name)
    return df_left_with_join_id


def repartition_dfs_for_join(spark_session, df_left_src, df_right_src, left_col_names_list, right_col_names_list, num_of_partitions=None):
    """
    This method was tested with left big table and right 'small' table'.
    Assuming that left table has skew/uneven data.
    :param spark_session - spark session
    :param df_left_src: spark dataframe
    :param df_right_src: spark datafarme
    :param left_col_names_list: names array of left columns to be joined (has the same order with columns on the right side).
    keeping the left columns
    :param right_col_names_list: names  array of right columns to be joined (has the same order with columns on the left side).
    :param num_of_partitions: the default is the number of the bins
    :return: tuple with df_left, left_col_name, df_right, right_col_name
    """
    left_col_name = 'l_col'
    right_col_name = 'r_col'

    df_left = df_left_src
    df_right = df_right_src

    if len(left_col_names_list) == 1:
        left_col_name = left_col_names_list[0]
        right_col_name = right_col_names_list[0]
    else:
        df_left = add_join_key(src_df=df_left_src, col_names_to_concat= left_col_names_list, concat_col_name=left_col_name)
        spark_session.sparkContext.setJobGroup(GROUP_ID, "add new column to right df")
        df_right = add_join_key(src_df=df_right_src, col_names_to_concat=right_col_names_list, concat_col_name=right_col_name)
        print "***********************************************************************************************************"
        print left_col_name, " is concat of keys: ", left_col_names_list
        print right_col_name, " is concat of keys: ", right_col_names_list
        print "***********************************************************************************************************"

    df_left.registerTempTable('left_table')
    df_right.registerTempTable('right_table')

    weights_query = '''SELECT %s ,count(1) as weight from left_table group by %s order by weight desc''' % (left_col_name, left_col_name)
    print_query(weights_query)
    df_join_key_weights = spark_session.sql(weights_query)

    # list of dict
    spark_session.sparkContext.setJobGroup(GROUP_ID, "collect rdd to python list (counting the number of repeated keys)")
    list_join_key_weights = [{left_col_name: i[left_col_name], 'weight': i['weight']}
                             for i in df_join_key_weights.select(left_col_name, 'weight').rdd.collect()]

    list_join_key_weights = bin_packing(weights_list=list_join_key_weights)

    print "*************************************************************************************"
    print "num of bins: ", len(list_join_key_weights)
    print "*************************************************************************************"

    if num_of_partitions is None:
        num_of_partitions = len(list_join_key_weights)
        print "setting num of partitions to ", num_of_partitions

    spark_session.sparkContext.setJobGroup(GROUP_ID, "create bin packing df")
    bin_packing_df = spark_session.createDataFrame(Row(**x) for x in list_join_key_weights)
    print "bin_packing_df "
    print bin_packing_df.printSchema()
    print "df_left:"
    print df_left.printSchema()

    df_left = add_join_id_column(spark_session=spark_session, df_src=df_left,
                                 df_weights=bin_packing_df,
                                 src_col_name=left_col_name,
                                 weights_col_name=left_col_name)

    df_left = df_left.repartition(num_of_partitions, BIN_ID, left_col_name)

    df_right = add_join_id_column(spark_session=spark_session, df_src=df_right,
                                  df_weights=bin_packing_df,
                                  src_col_name=right_col_name,
                                  weights_col_name=left_col_name)

    df_right = df_right.repartition(num_of_partitions, BIN_ID, right_col_name)

    return df_left, left_col_name, df_right, right_col_name


def left_join_with_skew_key(spark_session, df_left_src, df_right_src, left_col_names_list, right_col_names_list, brodcast_hint, num_of_partitions=None):
    """
       This method was tested with left big table and right 'small' table'.
       Assuming that left table has skew/uneven data.
       :param spark_session - spark session
       :param df_left_src: spark dataframe
       :param df_right_src: spark datafarme
       :param left_col_names_list: names array of left columns to be joined (has the same order with columns on the right side).
       keeping the left columns
       :param right_col_names_list: names  array of right columns to be joined (has the same order with columns on the left side).
       :param brodcast_hint: True/False. In case the left df is 'big' and the 'right' df is 'small'
       :param num_of_partitions: the default is the number of the bins
       :return:
       """

    (df_left, left_col_name, df_right, right_col_name) = repartition_dfs_for_join(spark_session, df_left_src, df_right_src, left_col_names_list, right_col_names_list, num_of_partitions)

    selected_columns = ['l' + "." + col_name for col_name in df_left.columns if col_name != BIN_ID] + \
                       ['r' + "." + col_name for col_name in df_right.columns if col_name not in right_col_names_list and col_name != BIN_ID]

    left_table_name = 'left_tbl'
    right_table_name = 'right_tbl'

    df_left.registerTempTable(left_table_name)
    df_right.registerTempTable(right_table_name)
    right_table_query = None

    if brodcast_hint is True:
        right_table_query = '''(SELECT /*+ BROADCAST (%s) */ * from (%s))''' % (right_table_name, right_table_name)
    else:
        right_table_query = '''(SELECT * from (%s))''' % right_table_name

    left_join_query = '''SELECT %s 
                            FROM %s as l 
                            LEFT OUTER JOIN %s as r 
                            ON l.%s=r.%s 
                            AND l.%s=r.%s''' % (', '.join(selected_columns),
                                                left_table_name,
                                                right_table_query,
                                                BIN_ID,
                                                BIN_ID,
                                                left_col_name,
                                                right_col_name)
    print_query(left_join_query)
    spark_session.sparkContext.setJobGroup(GROUP_ID, "left join query")
    return spark_session.sql(left_join_query)


