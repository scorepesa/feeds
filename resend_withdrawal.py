
data = [
     # {'msisdn':"254728334256", 'amount':327, 'withdrawal_id':'06827'},
]
import requests
import json
from utils import LocalConfigParser
import hashlib

withdraw_configs = LocalConfigParser.parse_configs("WITHDRAWAL")

url = withdraw_configs["url"]
token = withdraw_configs["oath_token"]
headers = {
 #'Authorization': 'Basic %s' % token,
 'Content-Type': 'application/json'
}

for json_data in data:

    hash_object = hashlib.md5(str(json_data.get('withdrawal_id')))
    payload = {
            'MSISDN': str(json_data.get("msisdn")),
            'amount': str(json_data.get("amount")),
            'reference': str(json_data.get('withdrawal_id')),
            'id': hash_object.hexdigest(),
            'user': {
                'MSISDN': str(json_data.get("msisdn")),
                'FirstName': '',
                'LastName': ''
            }
        }
    req_data = json.dumps(payload)
    print "Calling WITHDRAW URL: (%s, %s, %s) "%(url, payload, token)
    output = requests.post(url, data=req_data,
                auth=('admin', 'admin'), timeout=30, headers=headers)

    print "DONE", output, req_data

