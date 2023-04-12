import pyspark
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


class SF_Records:
    def __init__(self, ss, file_path):
        """
        Using file_path, initiate to assign values to attributes including
        ss, file_path and df.
        df is a Spark dataframe created from a csv file on file_path,
        where header='True' and inferSchema='True'
        """
        self.ss = ss
        self.file_path = file_path
        self.df = ss.read.csv(file_path, header=True, inferSchema=True)

    def return_count(self):
        return self.df.count()

    def get_max_weather_vals(self):
        """
        Return "received_date" (date from "received_datetime") and
        "max_weather_val"
        where max_weather_val is the largest value
        among  weather.0.value ... weather.24.value ordered by "received_date"
        in ascending order.
        """
        weather_cols = [f"`weather.{i}.value`" for i in range(25)]
        result = self.df.select(date_format("received_datetime", "yyyy-MM-dd")
                                .alias("received_date"),
                                greatest(*[col(c) for c in weather_cols])
                                .alias("max_weather_val")) \
            .groupBy("received_date") \
            .agg({"max_weather_val": "max"}) \
            .orderBy("received_date")
        return result.withColumn('received_date', to_date('received_date'))

    def filter_law_weather_max(self, id_val):
        """
        When "_id" is id_val,
        return "_id", "received_datetime", "analysis_neighborhood",
        "sensitive_call", "call_type_original_desc" and
        "max_weather_val" (from Q1)
        """
        result = self.df.filter(col("_id") == id_val)\
            .select("_id", "received_datetime", "analysis_neighborhood",
                    "sensitive_call", "call_type_original_desc")
        result = result.withColumn(
            'received_date', to_date('received_datetime'))
        # Add max_weather_val to result
        max_weather_vals = self.get_max_weather_vals()
        result = result.join(max_weather_vals, on='received_date', how='inner')
        result = result.select(
            "_id",
            "received_datetime",
            "analysis_neighborhood",
            "sensitive_call",
            "call_type_original_desc",
            col("max(max_weather_val)").alias("max_weather_val"))
        return result

    def return_diff_call_type(self, n):
        """
        Return the first n "received_datetime", "call_type_original_desc"
        and "call_type_final_desc" for records where "call_type_original_desc"
        and "call_type_final_desc"
        are different ordered by "received_datetime"
        """
        filtered_df = self.df.filter(
            col('call_type_original_desc') != col('call_type_final_desc'))

        # Sort the filtered dataframe by received_datetime in ascending order
        # and keep the first n rows
        sorted_df = filtered_df.orderBy('received_datetime').limit(n)

        # Select the required columns and return the resulting dataframe as a
        # list of rows
        result = sorted_df.select(
            'received_datetime',
            'call_type_original_desc',
            'call_type_final_desc')

        return result

    def return_ct_per_received_date(self, n):
        """
        Return the first "n" rows including count per "received_date"
        (Q1) ordered by "received_date"
        """
        result = self.df.select(date_format("received_datetime", "yyyy-MM-dd")
                                .alias("received_date")) \
            .groupBy("received_date").count() \
            .orderBy('received_date').limit(n)
        result = result.withColumn('received_date', to_date('received_date'))
        return result

    def return_daily_and_hist_neighborhood_ct(self, n):
        """
        When "analysis_neighborhood" which is not null,
        return the first n received_date, analysis_neighborhood,
        count (the number of records for the date and analysis_neighborhood),
        and count_n_days_ago (the number of records for n days prior to
        the date in analysis_neighborhood) ordered by count (descending),
        count_n_days_ago (ascending), and received_date (ascending).
        """
        w = Window.partitionBy("analysis_neighborhood").orderBy(
            col("received_datetime").asc_nulls_first())
        result = self.df.select(
            "analysis_neighborhood", date_format(
                "received_datetime", "yyyy-MM-dd").alias("received_date")) \
            .filter(
            self.df.analysis_neighborhood.isNotNull()) .groupBy(
                "received_date", "analysis_neighborhood") .agg(
                    count("*").alias("count")) .withColumn(
                        "count_n_days_ago", lag(
                            "count", n).over(
                                Window.partitionBy("analysis_neighborhood")
                                .orderBy(col("received_date")))) \
            .orderBy(col("count").desc(),
                     col("count_n_days_ago")
                     .asc(), col("received_date").asc()) .limit(n)

        result = result.withColumn('received_date', to_date('received_date'))
        result = result.select("analysis_neighborhood",
                               "received_date", "count", "count_n_days_ago")
        return result
