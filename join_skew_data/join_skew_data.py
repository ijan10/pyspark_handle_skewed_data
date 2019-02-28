import pyspark.sql.functions as func

from pyspark.sql.types import StringType


from operator import itemgetter
import traceback
from pyspark.sql import Row
from bin_packing_utils import JoinBinPackingUtil


class JoinSkewWithBinPacking:

    BIN_ID = JoinBinPackingUtil.BIN_ID
    GROUP_ID = 'bin_packing'

    def __init__(self):
        pass

    @staticmethod
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
            df_left = JoinBinPackingUtil.add_join_key(src_df=df_left_src, col_names_to_concat=left_col_names_list, concat_col_name=left_col_name)
            spark_session.sparkContext.setJobGroup(JoinSkewWithBinPacking.GROUP_ID, "add new column to right df")
            df_right = JoinBinPackingUtil.add_join_key(src_df=df_right_src, col_names_to_concat=right_col_names_list, concat_col_name=right_col_name)
            print "***********************************************************************************************************"
            print left_col_name, " is concat of keys: ", left_col_names_list
            print right_col_name, " is concat of keys: ", right_col_names_list
            print "***********************************************************************************************************"

        df_left.registerTempTable('left_table')
        df_right.registerTempTable('right_table')

        weights_query = '''SELECT %s ,count(1) as weight from left_table group by %s order by weight desc''' % (left_col_name, left_col_name)
        JoinBinPackingUtil.print_query(weights_query)
        df_join_key_weights = spark_session.sql(weights_query)

        # list of dict
        spark_session.sparkContext.setJobGroup(JoinSkewWithBinPacking.GROUP_ID, "collect rdd to python list (counting the number of repeated keys)")
        list_join_key_weights = [{left_col_name: i[left_col_name], 'weight': i['weight']}
                                 for i in df_join_key_weights.select(left_col_name, 'weight').rdd.collect()]

        list_join_key_weights = JoinBinPackingUtil.bin_packing(weights_list=list_join_key_weights)

        print "*************************************************************************************"
        print "num of bins: ", len(list_join_key_weights)
        print "*************************************************************************************"

        if num_of_partitions is None:
            num_of_partitions = len(list_join_key_weights)
            print "setting num of partitions to ", num_of_partitions

        spark_session.sparkContext.setJobGroup(JoinSkewWithBinPacking.GROUP_ID, "create bin packing df")
        bin_packing_df = spark_session.createDataFrame(Row(**x) for x in list_join_key_weights)
        print "bin_packing_df "
        print bin_packing_df.printSchema()
        print "df_left:"
        print df_left.printSchema()

        df_left = JoinBinPackingUtil.add_join_id_column(spark_session=spark_session, df_src=df_left,
                                     df_weights=bin_packing_df,
                                     src_col_name=left_col_name,
                                     weights_col_name=left_col_name)

        df_left = df_left.repartition(num_of_partitions, JoinSkewWithBinPacking.BIN_ID, left_col_name)

        df_right = JoinBinPackingUtil.add_join_id_column(spark_session=spark_session, df_src=df_right,
                                      df_weights=bin_packing_df,
                                      src_col_name=right_col_name,
                                      weights_col_name=left_col_name)

        df_right = df_right.repartition(num_of_partitions, JoinSkewWithBinPacking.BIN_ID, right_col_name)

        return df_left, left_col_name, df_right, right_col_name

    @staticmethod
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

        (df_left, left_col_name, df_right, right_col_name) = JoinSkewWithBinPacking.repartition_dfs_for_join(spark_session, df_left_src, df_right_src, left_col_names_list, right_col_names_list,
                                                                                      num_of_partitions)

        selected_columns = ['l' + "." + col_name for col_name in df_left.columns if col_name != JoinSkewWithBinPacking.BIN_ID] + \
                           ['r' + "." + col_name for col_name in df_right.columns if col_name not in right_col_names_list and col_name != JoinSkewWithBinPacking.BIN_ID]

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
                                                    JoinSkewWithBinPacking.BIN_ID,
                                                    JoinSkewWithBinPacking.BIN_ID,
                                                    left_col_name,
                                                    right_col_name)
        JoinBinPackingUtil.print_query(left_join_query)
        spark_session.sparkContext.setJobGroup(JoinSkewWithBinPacking.GROUP_ID, "left join query")
        return spark_session.sql(left_join_query)

