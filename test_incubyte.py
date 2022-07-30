import unittest
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from main import test_incubyte_spark

class SparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[*]").appName("Testing").getOrCreate())


    @classmethod
    def stopClass(cls):
        cls.spark.stop()

    def test_vaccine(self):
        input_schema = StructType([
            StructField('Name', StringType(), True),
            StructField('VaccinationType', StringType(), True),
            StructField('VaccinationDate', StringType(), True),
            StructField('Free or Paid', StringType(), True)
        ])

        input_data = [('Test1', "ABC", "2022-08-08","F"),('Test2', "XYZ", "2022-08-09","F"),('Test3', "HUI", "2021-08-08","P"),('Test4', "IOP", "2022-05-06","F"),('Test5', "IOP", "2021-04-06","F")]
        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)
        vaccine_count, percentage_vaccinated = test_incubyte_spark(input_df)
        self.assertEqual(vaccine_count,5)
        self.assertEqual(percentage_vaccinated, "29.0%")

if __name__ == '__main__':
    unittest.main()