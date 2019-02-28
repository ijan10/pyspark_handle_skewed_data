import pyspark.sql.functions as func

from pyspark.sql.types import StringType
from operator import itemgetter
import traceback
from pyspark.sql import Row


class JoinBinPackingUtil:
    BIN_ID = 'join_id'

    def __init__(self):
        pass

    @staticmethod
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

    @staticmethod
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
                    if (item_inner.get(JoinBinPackingUtil.BIN_ID) is None) and (item_inner['weight'] + sum_of_weight <= max_weight_in_bin):
                        item_inner[JoinBinPackingUtil.BIN_ID] = bin_id
                        sum_of_weight += item_inner['weight']

                bin_id += 1

        return weights_list

    @staticmethod
    def print_query(query_string):
        """
        :param query_string:
        :return:
        """
        print "*******************************************************************************************"
        print "Run Query:"
        print query_string
        print "*******************************************************************************************"

    @staticmethod
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
        selected_columns = ['l' + "." + col_name for col_name in df_src.columns] + ['r.' + JoinBinPackingUtil.BIN_ID]

        query_add_join_id = '''SELECT %s 
                                   FROM %s as l 
                                   LEFT OUTER JOIN (SELECT /*+ BROADCAST (%s) */ * from (%s)) as r 
                                   ON l.%s = r.%s''' % (', '.join(selected_columns), src_table_name, weights_table_name, weights_table_name, src_col_name, weights_col_name)

        JoinBinPackingUtil.print_query(query_add_join_id)

        df_left_with_join_id = spark_session.sql(query_add_join_id)
        spark_session.catalog.dropTempView(src_table_name)
        return df_left_with_join_id
