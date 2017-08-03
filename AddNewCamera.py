#!/usr/bin/python

import httplib2
import json
import csv
import os
import sys  
import time
import urllib
import dateutil
import dateutil.parser
import datetime
import ssl

reload(sys)  
sys.setdefaultencoding('utf-8')
ssl._create_default_https_context = ssl._create_unverified_context

APPLICATION_ID = "pacX2E8eBcQw4MLO0xWnkjIR0m3sRe4lOCBvOvPx"
REST_API_KEY = "9ej9Rjfx4QnCBvehtUbt0rHhgNtCr0XhBGa0frJM" #REST API Key
MASTER_KEY = "ZcF03KKqCddYbYtGc4hIvb8Okh9giGU0sz703hln" #Master Key
CLASSES = "AddNewCamera" #Enter comma seperated classnames here, ex "User,Role" etc! Don't add space after/before comma.
skip = 0 # Skip these many rows, used in skip = skip_count*limit
limit = 16000 #limit number of rows per call - Max is 1000
filter_sorted = json.dumps({"sorted": {"$exists": False}})

def definition():
    global files_dir_path
    files_dir_path = os.getcwd()

def getData(app_id, rest_api_key, api_endpoint, master_key=None, limit=200, order="-createdAt", skip=None, filter_json=filter_sorted, api_version=1):

    con = httplib2.HTTPSConnection('parseapi.back4app.com', 443)
    con.connect()

    header_dict = {'X-Parse-Application-Id': app_id,
                   'X-Parse-REST-API-Key': rest_api_key
                   }


    if master_key is not None:
        header_dict['X-Parse-Master-Key'] = master_key

    params_dict = {}
    if order is not None:
        params_dict['order'] = order
    if limit is not None:
        params_dict['limit'] = limit
    if skip is not None:
        params_dict['skip'] = skip
    if filter_json is not None:
        params_dict['where'] = filter_json

    params = urllib.urlencode(params_dict)
    con.request('GET', '/%s?%s' % (api_endpoint, params), '', header_dict)

    try:
        response = json.loads(con.getresponse().read())
    except Exception as e:
        response = None
        raise e

    return response

def getWhoCalled():

    definition()

    class_list = CLASSES.split(",") #For multiple classes!
    DEFAULT_CLASSES = {'User': 'users', 'Role': 'roles', 'File': 'files', 'Events': 'events', 'Installation': 'installations'}

    for classname in class_list:
        csv_file_name = "AddNewCamera_" + datetime.datetime.now().strftime("%B%d") + ".csv"
        csv_file_path = os.path.join(files_dir_path, csv_file_name)

        results = {'results': []}
        object_count = 0
        skip_count = 0

        if classname not in DEFAULT_CLASSES.keys():
            endpoint = '%s/%s' % ('classes', classname)
        else:
            endpoint = DEFAULT_CLASSES[classname]

        sys.stdout.write(' Fetching %s table data - ' % classname)
        sys.stdout.flush()
       
        while True:
            startTimer = time.clock()
            skip = skip_count*limit

            response = getData(APPLICATION_ID, REST_API_KEY, endpoint, master_key=MASTER_KEY, limit = limit, skip = skip)
       
            if 'results' in response.keys() and len(response['results']) > 1:
                object_count += len(response['results'])
                skip_count = skip_count+1
                results['results'].extend(response['results'])
                parse_done = time.clock() - startTimer
                break

            else:
                parse_done = time.clock() - startTimer
                break


        newList = []
        for x in results["results"]:
            # Uncomment below line to manually set sequence if you know column titles.
            if "heading" in x and "sorted" not in x and x["type"] == "regular" and x["lat"] != 0 and x["long"] != 0:
           	#f.writerow([x["lat"], x["long"], x["type"], x["heading"], dateutil.parser.parse(x["createdAt"]).strftime("%d-%b-%Y, %H:%M:%S")])
            #f.writerow(x.values())

                x["lat2"] = x.pop("long")
                x["long"] = x.pop("lat")
                x["lat"] = x.pop("lat2")
                x["sorted"] = True
                x["type"] = "mobile"
                newList.append(x)

        for x in newList:
            con = httplib2.HTTPSConnection('parseapi.back4app.com', 443)
            con.connect()
            fullurl = '/classes/AddNewCamera/' + x["objectId"]
            newdict = {"lat" : x["lat"], "long" : x["long"], "type" : "mobile", "sorted" : True}
          
            header_dict = {'X-Parse-Application-Id': APPLICATION_ID,
                   'X-Parse-REST-API-Key': REST_API_KEY,
                   "Content-Type" : "application/json"
                   }
            params = urllib.urlencode(newdict)
            con.request('PUT', fullurl, json.dumps(newdict), header_dict)

            result = con.getresponse().read()


def returnOnlyPath():
    csv_file_name = "WhoCalled_" + datetime.datetime.now().strftime("%B%d") + ".csv"
    csv_file_path = os.path.join(files_dir_path, csv_file_name)
    result = {
        'filePath': csv_file_path,
        'fileName': csv_file_name
    }
    return result
