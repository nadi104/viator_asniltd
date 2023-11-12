import json
from collections import defaultdict
import http.client

from pymongo import MongoClient

with open('data_2023/api_code_bkns32.json', 'r', encoding="utf8") as file:
    data = json.load(file)

MONGODB_URL = "mongodb+srv://ashini1991:d93GD47lz2M1is85@db-mongodb-nyc1-36942-07e78134.mongo.ondigitalocean.com/kafka_mongo?tls=true&authSource=admin&replicaSet=db-mongodb-nyc1-36942"

client = MongoClient(MONGODB_URL)
db = client["kafka_mongo"]
collection = db["test_jan_march"]


def calculate_total_request_2023(mydata):
    date_counts = defaultdict(lambda: defaultdict(int))
    request_default = ['AvbReq', 'sold_out', 'BKReq', 'BKAMNDReq', 'BKCReq']
    for item in mydata:
        vst_date = item.get('vst_date')
        req_type = item.get('req_type')
        status = item.get('status')

        date_counts[vst_date][req_type] += 1

        if req_type == 'AvbReq':
            status_parts = status.split("/")
            first_status_part = status_parts[0].strip()
            if first_status_part == "SOLD_OUT":
                date_counts[vst_date]['sold_out'] += 1

    for date, counts in date_counts.items():
        for request_type in request_default:
            if request_type not in counts:
                counts[request_type] = 0

    formatted_data = []
    for date, counts in date_counts.items():
        formatted_counts = {}
        for key, value in counts.items():
            if isinstance(value, set):
                formatted_value = list(value)
            else:
                formatted_value = value
            formatted_counts[key] = formatted_value
        formatted_dict = {"vst_date": date}
        formatted_dict.update(formatted_counts)
        formatted_data.append(formatted_dict)

    with open('data_2023/102_req_type.json', 'w') as output_file:
        json.dump(formatted_data, output_file)


def calculate_total_request_jan_march(mydata):
    date_counts = defaultdict(lambda: defaultdict(int))
    request_default = ['AvbReq', 'sold_out', 'BKReq', 'BKAMNDReq', 'BKCReq']
    for item in mydata:
        vst_date = item.get('vst_date')
        req_type = item.get('api')
        status = item.get('status')

        date_counts[vst_date][req_type] += 1

        if req_type == 'AvbReq':
            status_parts = status.split("/")
            first_status_part = status_parts[0].strip()
            if first_status_part == "SOLD_OUT":
                date_counts[vst_date]['sold_out'] += 1

    for date, counts in date_counts.items():
        for request_type in request_default:
            if request_type not in counts:
                counts[request_type] = 0

    formatted_data = []
    for date, counts in date_counts.items():
        formatted_counts = {}
        for key, value in counts.items():
            if isinstance(value, set):
                formatted_value = list(value)
            else:
                formatted_value = value
            formatted_counts[key] = formatted_value
        formatted_dict = {"vst_date": date}
        formatted_dict.update(formatted_counts)

        document = collection.find_one({'vst_date': date})
        if document:
            mongoid = document.get('_id')
            Avbreq = document.get('AvbReq') + formatted_counts['AvbReq']
            sold_out = document.get('sold_out') + formatted_counts['sold_out']
            BKreq = document.get('BKReq') + formatted_counts['BKReq']
            BKCReq = document.get('BKCReq') + formatted_counts['BKCReq']
            BKAMNDReq = document.get('BKAMNDReq') + formatted_counts['BKAMNDReq']
            query = {'_id': mongoid}
            update_data = {
                '$set': {
                    'AvbReq': Avbreq,
                    'sold_out': sold_out,
                    'BKReq': BKreq,
                    'BKCReq': BKCReq,
                    'BKAMNDReq': BKAMNDReq
                }
            }

            result = collection.update_one(query, update_data)
        else:
            collection.insert_one(formatted_dict)

        formatted_data.append(formatted_dict)


def calculate_missing_viator(mydata):
    date_counts = defaultdict(lambda: defaultdict(int))
    request_default = ['AvbReq', 'sold_out', 'BKReq', 'BKAMNDReq', 'BKCReq']
    for item in mydata:
        vst_date = item.get('vst_date')
        req_type = item.get('api')
        status = item.get('status')
        if req_type in request_default:
            date_counts[vst_date][req_type] += 1

        if req_type == 'AvbReq':
            status_parts = status.split("/")
            first_status_part = status_parts[0].strip()
            if first_status_part == "SOLD_OUT":
                date_counts[vst_date]['sold_out'] += 1

    for date, counts in date_counts.items():
        for request_type in request_default:
            if request_type not in counts:
                if request_type in request_default:
                    counts[request_type] = 0

    formatted_data = []
    for date, counts in date_counts.items():
        formatted_counts = {}
        for key, value in counts.items():
            if isinstance(value, set):
                formatted_value = list(value)
            else:
                formatted_value = value
            formatted_counts[key] = formatted_value
        formatted_dict = {"vst_date": date}
        formatted_dict.update(formatted_counts)
        document = collection.find_one({'vst_date': date})
        if document:
            mongoid = document.get('_id')
            Avbreq = document.get('AvbReq') + formatted_counts['AvbReq']
            sold_out = document.get('sold_out') + formatted_counts['sold_out']
            BKreq = document.get('BKReq') + formatted_counts['BKReq']
            BKCReq = document.get('BKCReq') + formatted_counts['BKCReq']
            BKAMNDReq = document.get('BKAMNDReq') + formatted_counts['BKAMNDReq']
            query = {'_id': mongoid}
            update_data = {
                '$set': {
                    'AvbReq': Avbreq,
                    'sold_out': sold_out,
                    'BKReq': BKreq,
                    'BKCReq': BKCReq,
                    'BKAMNDReq': BKAMNDReq
                }
            }

            result = collection.update_one(query, update_data)
        else:
            collection.insert_one(formatted_dict)
        formatted_data.append(formatted_dict)

    # with open('missing_data.json', 'w') as output_file:
    #     json.dump(formatted_data, output_file)


def calculate_total_request_old(mydata):
    date_counts = defaultdict(lambda: defaultdict(int))
    request_default = ['AvbReq', 'sold_out', 'BKReq', 'BKAMNDReq', 'BKCReq']
    for item in mydata:
        vst_date = item.get('vst_date')
        req_type = item.get('api')
        status = item.get('status')
        if req_type in request_default:
            date_counts[vst_date][req_type] += 1

        if req_type == 'AvbReq':
            status_parts = status.split("/")
            first_status_part = status_parts[0].strip()
            if first_status_part == "SOLD_OUT":
                date_counts[vst_date]['sold_out'] += 1

    for date, counts in date_counts.items():
        for request_type in request_default:
            if request_type not in counts:
                if request_type in request_default:
                    counts[request_type] = 0

    formatted_data = []
    for date, counts in date_counts.items():
        formatted_counts = {}
        for key, value in counts.items():
            if isinstance(value, set):
                formatted_value = list(value)
            else:
                formatted_value = value
            formatted_counts[key] = formatted_value
        formatted_dict = {"vst_date": date}
        formatted_dict.update(formatted_counts)
        formatted_data.append(formatted_dict)

    with open('data_2022/122_req_type.json', 'w') as output_file:
        json.dump(formatted_data, output_file)


def update_api_count(mydata):
    for item in mydata:
        vst_date = item.get('vst_date')
        api = item.get('api')
        documents = collection.find_one({'vst_date': vst_date, 'api': api})
        if documents:
            mongo_id = documents.get('_id')
            bkreq = item.get('BKReq') + documents.get('BKReq')
            bkcreq = item.get('BKCReq') + documents.get('BKCReq')
            bkamndreq = item.get('BKAMNDReq')+documents.get('BKAMNDReq')
            query = {'_id': mongo_id}
            update_data = {
                '$set': {
                    'BKReq': bkreq,
                    'BKCReq': bkcreq,
                    'BKAMNDReq': bkamndreq
                }
            }
            result = collection.update_one(query, update_data)
        else:
            collection.insert_one(item)


def calculate_api_count(mydata):
    api_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    request_default = ['AvbReq', 'sold_out', 'BKReq', 'BKAMNDReq', 'BKCReq']
    for item in mydata:
        vst_date = item.get('vst_date')
        req_type = item.get('api')
        status = item.get('status')
        if req_type == 'AvbReq':
            api_code = item.get('booking_id')
            api_counts[vst_date][api_code][req_type] += 1

            status_parts = status.split("/")
            first_status_part = status_parts[0].strip()
            if first_status_part == "SOLD_OUT":
                api_counts[vst_date][api_code]['sold_out'] += 1
        elif req_type == 'BKCReq':
            booking_code = str(item.get('booking_id'))
            api_code = get_api_code(booking_code)
            api_counts[vst_date][api_code][req_type] += 1
        else:
            api_code = item.get('api_id')
            api_counts[vst_date][api_code][req_type] += 1

    formatted_data = []
    for date, counts in api_counts.items():
        for api, value in counts.items():
            formatted_dict = {
                "vst_date": date,
                "api": api
            }
            for req_type in request_default:
                formatted_dict[req_type] = value.get(req_type, 0)
            # formatted_dict.update(value)
            formatted_data.append(formatted_dict)

    with open('data_2023/api_code_bkns32.json', 'w') as api_output:
        json.dump(formatted_data, api_output)


def get_api_code(booking_code):
    conn = http.client.HTTPSConnection("starfish-app-eay5q.ondigitalocean.app")
    payload = json.dumps({
        "id": booking_code
    })
    headers = {
        'Content-Type': 'application/json'
    }
    conn.request("POST", "/findbooking_by_id", payload, headers)
    res = conn.getresponse()
    data_BKCReq = res.read()
    jason_data = json.loads(data_BKCReq)
    data_list = [item["json_req"]["data"] for item in jason_data]
    supplier_product_codes = [item.get('SupplierProductCode', '') for item in data_list]
    supplier_product_codes = set(supplier_product_codes)
    api_code = ""
    for code in supplier_product_codes:
        api_code = code
    return api_code


#calculate_api_count(data)
update_api_count(data)
#calculate_total_request_2023(data)
# calculate_total_request_old(data)
# calculate_missing_viator(data)
# calculate_total_request_jan_march(data)
