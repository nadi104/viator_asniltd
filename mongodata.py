from pymongo import MongoClient
import json
from datetime import datetime

MONGODB_URL = "mongodb+srv://ashini1991:d93GD47lz2M1is85@db-mongodb-nyc1-36942-07e78134.mongo.ondigitalocean.com/kafka_mongo?tls=true&authSource=admin&replicaSet=db-mongodb-nyc1-36942"

client = MongoClient(MONGODB_URL)
db = client["kafka_mongo"]
collection = db["jan_mar_bkns"]


def export_data_for_date_range(st_date, en_date, month):
    # parsed_st_date = datetime.strptime(st_date, "%Y-%m-%d")
    # formatted_st_date = parsed_st_date.strftime("%Y-%m-%dT00:00:00.000+00:00")
    # print(formatted_st_date)
    # parsed_en_date = datetime.strptime(en_date, "%Y-%m-%d")
    # formatted_en_date = parsed_en_date.strftime("%Y-%m-%dT00:00:00.000+00:00")
    # print(formatted_en_date)
    query = {
        "vst_date": {
            "$gte": st_date,
            "$lte": en_date
        }
    }

    fields = {
        "booking_id": 1,
        "trvel_date": 1,
        "vst_date": 1,
        "status": 1,
        "req_type": 1,
        "supp_name": 1,
        "api": 1,
        "time_stamp": 1,
        "api_id": 1
    }

    documents = list(collection.find(query, fields))

    filename = f"data_2023/{month}1bkns.json"
    with open(filename, "w") as json_file:
        json.dump(documents, json_file, default=str)


if __name__ == "__main__":
    #year = 2023

    # for month in range(1, 13):
    #     start_date = datetime(year, month, 1)
    #     start_date = start_date.strftime("%Y-%m-%d")
    #     if month == 2:
    #         end_date = datetime(year, month, 28)
    #     elif month in {4, 6, 9, 11}:
    #         end_date = datetime(year, month, 30)
    #     else:
    #         end_date = datetime(year, month, 31)
    #     end_date = end_date.strftime("%Y-%m-%d")
    #
    #     print(start_date)
    #     print(end_date)

    #export_data_for_date_range("2023-10-01", "2023-10-15", 11)
    export_data_for_date_range("2023-02-01", "2023-02-15", 2)

