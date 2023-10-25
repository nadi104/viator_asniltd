import json
from collections import defaultdict
import http.client

with open('4.json', 'r') as file:
    data = json.load(file)


def calculate_total_request(mydata):
    date_counts = defaultdict(lambda: defaultdict(int))
    request_default = ['AvbReq', 'sold_out', 'BKReq', 'BKAMNDReq', 'BKCReq']
    for item in mydata:
        vst_date = item.get('vst_date')
        print(vst_date)
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

    with open('11_req_type_test.json', 'w') as output_file:
        json.dump(formatted_data, output_file)


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
        formatted_data.append(formatted_dict)

    with open('missing_data.json', 'w') as output_file:
        json.dump(formatted_data, output_file)


def calculate_api_count(mydata):
    api_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for item in mydata:
        vst_date = item.get('vst_date')
        req_type = item.get('req_type')
        if req_type == 'AvbReq':
            api_code = item.get('booking_id')
            api_counts[vst_date][api_code][req_type] += 1
        elif req_type == 'BKCReq':
            booking_code = str(item.get('booking_id'))
            api_code = get_api_code(booking_code)
            api_counts[vst_date][api_code][req_type] += 1
        else:
            api_code = item.get('api_id')
            api_counts[vst_date][api_code][req_type] += 1

    with open('api_code_count_4.json', 'w') as api_output:
        json.dump(api_counts, api_output)


def get_api_code(booking_code):
    # formatted_booking_id = "{:04d}".format(booking_code)
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
    # print(data.decode("utf-8"))
    jason_data = json.loads(data_BKCReq)
    # Extract SupplierProductCode values
    data_list = [item["json_req"]["data"] for item in jason_data]

    supplier_product_codes = [item.get('SupplierProductCode', '') for item in data_list]
    supplier_product_codes = set(supplier_product_codes)
    # Print the extracted SupplierProductCode values
    api_code: str = ""
    for code in supplier_product_codes:
        api_code = code
    return api_code


calculate_api_count(data)
#calculate_total_request(data)
#calculate_missing_viator(data)


# for vst_date, count_dict in date_counts.items():
#     print(f"Date: {vst_date}")
#     for key, count in count_dict.items():
#         print(f"{key}: {count}")


# with open('api_code_count_4.json', 'w') as api_output:
#     req_api_count = calculate_api_count()
#     json.dump(req_api_count, api_output)
