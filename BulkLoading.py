import snowflake.connector
import pandas as pd

def get_dataType(dtype):
    if dtype == "int64":
        return "INT"
    elif dtype == "float64":
        return "FLOAT"
    else:
        return "STRING"

connection = snowflake.connector.connect(
    user="YOUR USERNAME",
    password="YOUR PASSWORD",
    account = "YOU ACCOUNT"
    warehouse="YOUR WAREHOUSE",
    database="YOUR DATABASE",
    #schema="TESTNEW",
    #stage="TESTSTAGE4",
)

cursor = connection.cursor()

cursor.execute("CREATE SCHEMA AAAAFINALTestFINAL")
cursor.execute("CREATE STAGE teststage4")
#cursor.execute("SHOW STAGES LIKE 'TESTSTAGE4'")
#print(cursor.fetchall())

#data = [["13F.parquet", "parquet"]]

# Change file names to not include space
#data = [["PARA.csv", "Paramount"], ["PENN.csv", "PennEnt"], ["QBStats2024.csv", "QBStats"],
#        ["RBStats2024.csv", "RBStats"], ["WRStats2024.csv", "WRStats"], ["SampleData.xlsx", "SampleData"]]

# Change file names to not include space
data = [["nissan-dataset.csv", "nissan-dataset-csv"], ["nissan-dataset1.xlsx", "nissan-dataset1-worksheet"], ["NissanContingencyTable.csv", "Nissan Contingency Table"],
        ["sums.csv", "sums"], ["LifeExpectancyData.csv", "LED"], ["NFLCorrelationMatrix.csv", "NFL"], ["ContingencyTable_Age&Model.csv", "A&M"],
        ["ContingencyTable_Age&Color.csv", "A&C"], ["CH2_ContingencyTable.csv", "CH2"],
        ["CNBC.csv", "CNBC"], ["FOX.csv", "FOX"], ["GOOG.csv", "GOOGLE"], ["MasterData.csv", "MasterNFL"],
        ["PARA.csv", "Paramount"], ["PENN.csv", "PennEnt"], ["QBStats2024.csv", "QBStats"],
        ["RBStats2024.csv", "RBStats"], ["WRStats2024.csv", "WRStats"], ["SampleData.xlsx", "SampleData"],
        ["13F.parquet", "parquet"],  ["MasterData.parquet", "NFLParquet"], ["MergedDF.parquet", "MergedDFPar"]]

for file, tableName in data:
    if "csv" in file:
        df = pd.read_csv(file)
    elif "xlsx" in file:
        df = pd.read_excel(file)
        file = file.replace("xlsx", "csv")
        df.to_csv(f"{file}")
    else:
        df = pd.read_parquet(file)
        #file = file.replace("parquet", "csv")
        #df.to_parquet(f"{file}")

    columns = ', '.join([f'"{column}" {get_dataType(df[column].dtype)}' for column in df.columns])

    createTable = f'CREATE OR REPLACE TABLE "{tableName}" ({columns})'
    cursor.execute(createTable)

    put = f"PUT file://{file} @testStage4/{file}"

    cursor.execute(put)

    if "csv" in file:
        print(f"Loading {file}")
        cursor.execute(f"""
            COPY INTO "{tableName}"
            FROM @testStage4/{file}
            FILE_FORMAT = (TYPE = 'CSV')
            ON_ERROR = CONTINUE;
        """)
        print(f"{file} loaded succesfully")
    else:
        print(f"Loading {file}")
        cursor.execute(f"""
            COPY INTO {tableName}
            FROM @testStage4/{file}
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME
            ON_ERROR = CONTINUE
        """)
        print(f"{file} loaded succesfully")

cursor.close()