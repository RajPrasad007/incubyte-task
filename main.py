from pyspark.sql import SparkSession
import pandas
spark = SparkSession.builder.appName("IncubyteTask").getOrCreate()

def stuff(country_name):
    if country_name == "AUS":
        # Converting AUS.xlsx to AUS.csv
        read_excel = pandas.read_excel(f"SampleInputData/{country_name}.xlsx")
        read_excel.to_csv(f"SampleInputData/{country_name}.csv",index = None, header=True)
        df = spark.read.csv(f"SampleInputData/{country_name}.csv",header=True)
    elif country_name in ["IND","USA"]:
        df = spark.read.csv(f"SampleInputData/{country_name}.csv",header=True)
    else:
        print("Only accept country names like:- AUS, IND and USA!")
        exit(1)
    
    vaccine_column_name = [col for col in df.columns if col.startswith("Vaccin")]
    date_of_vaccine = df.select(vaccine_column_name[0]).rdd.flatMap(lambda x: x).collect()
    date_of_vaccine_no_dups = ','.join(set(date_of_vaccine))
    
    # counting the number of vaccinations through date of vaccination
    vaccine_count = 0
    for i in range(len(date_of_vaccine)):
        if date_of_vaccine[i] is not None:
            vaccine_count += 1
    
    # Suppose, Total Population for AUS is 58, USA is 86 and IND is 189.
    if country_name == "AUS":
        total_population = 58
        cal = 100*total_population/vaccine_count
        final_result = str(round(cal,0)/100) + "%"
    elif country_name == "IND":
        total_population = 189
        cal = 100*total_population/vaccine_count
        final_result = str(round(cal,0)/100) + "%"
    else:
        total_population = 86
        cal = 100*total_population/vaccine_count
        final_result = str(round(cal,0)/100) + "%"
    
    return country_name,vaccine_count,date_of_vaccine_no_dups,final_result

# Only for testing purposes-->
def test_incubyte_spark(df):
    vaccine_column_name = [col for col in df.columns if col.startswith("Vaccin")]
    date_of_vaccine = df.select(vaccine_column_name[0]).rdd.flatMap(lambda x: x).collect()
    date_of_vaccine_no_dups = ','.join(set(date_of_vaccine))
    vaccine_count = 0
    for i in range(len(date_of_vaccine)):
        if date_of_vaccine[i] is not None:
            vaccine_count += 1
    # Suppose population is 145
    total_population = 145
    cal = 100*total_population/vaccine_count
    final_result = str(round(cal,0)/100) + "%"
    return vaccine_count,final_result
# Only for testing purposes-->

if __name__ == "__main__":
    country_name_1,no_of_vaccine_1,country_name_dov_1,vaccinated_1 = stuff("AUS")
    country_name_2,no_of_vaccine_2,country_name_dov_2,vaccinated_2 = stuff("IND")
    country_name_3,no_of_vaccine_3,country_name_dov_3,vaccinated_3 = stuff("USA")
    columns_1 = ["CountryName","VaccinationType","No. of Vaccinations"]
    columns_2 = ["CountryName",'% Vaccinated']
    data_1 = [(f"{country_name_1}",f"{country_name_dov_1}",f"{no_of_vaccine_1}"),
            (f"{country_name_2}",f"{country_name_dov_2}",f"{no_of_vaccine_2}"),
            (f"{country_name_3}",f"{country_name_dov_3}",f"{no_of_vaccine_3}")]
    data_2 = [(f"{country_name_1}",f"{vaccinated_1}"),
            (f"{country_name_2}",f"{vaccinated_2}"),
            (f"{country_name_3}",f"{vaccinated_3}")]
    
    print("\033[92m[!]\033[0m Wait, Creating Metric 1")
    rdd_1 = spark.sparkContext.parallelize(data_1)
    result_df_1 = rdd_1.toDF(columns_1)

    print("\033[92m[!]\033[0m Wait, Creating Metric 2")
    rdd_2 = spark.sparkContext.parallelize(data_2)
    result_df_2 = rdd_2.toDF(columns_2)

    # Creating Metric_1.csv
    result_df_1.toPandas().to_csv("result/Metric1.csv")

    # Creating Metric_2.csv
    result_df_2.toPandas().to_csv("result/Metric2.csv")

    print("\033[92m[+]\033[0m Done, Please look inside the result directory!")