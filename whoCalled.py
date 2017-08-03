#!/usr/bin/python

import httplib
import json
import csv
import os
import sys  
import time
import urllib
import dateutil
import dateutil.parser
import datetime

reload(sys)  
sys.setdefaultencoding('utf-8')

APPLICATION_ID = "kCnqm5CI6ZRt83Ky4iI7XunzFCz8uvLAJ5HJCCrR"
REST_API_KEY = "wxI6JZFAwbJsVR0O0IUiYCgSTlJuc1sgcAzh2lcb" #REST API Key
MASTER_KEY = "ZcF03KKqCddYbYtGc4hIvb8Okh9giGU0sz703hln" #Master Key
CLASSES = "Caller" #Enter comma seperated classnames here, ex "User,Role" etc! Don't add space after/before comma.
skip = 0 # Skip these many rows, used in skip = skip_count*limit
limit = 200 #limit number of rows per call - Max is 1000

def definition():
    global files_dir_path
    files_dir_path = os.getcwd()

def getData(app_id, rest_api_key, api_endpoint, master_key=None, limit=100, order="-createdAt", skip=None, filter_json=None, api_version=1):

    con = httplib.HTTPSConnection('parseapi.back4app.com', 443)
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
    except Exception, e:
        response = None
        raise e

    return response

def getWhoCalled():

    definition()
    print "*** Requesting...  ***\n"

    class_list = CLASSES.split(",") #For multiple classes!
    DEFAULT_CLASSES = {'User': 'users', 'Role': 'roles', 'File': 'files', 'Events': 'events', 'Installation': 'installations'}

    for classname in class_list:
        csv_file_name = "WhoCalled_" + datetime.datetime.now().strftime("%B%d") + ".csv"
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
                print ' Got: %.0f records in %.4f secs\n' % (object_count, parse_done)
                break

            else:
                parse_done = time.clock() - startTimer
                print ' Got: %.0f records in %.4f secs\n' % (object_count, parse_done)
                break

        print 'Generating csv... '

        f = csv.writer(open(csv_file_path, 'w'))
        f.writerow(["phoneNumber","label","createdAt"])
        for x in results["results"]:
            # Uncomment below line to manually set sequence if you know column titles.
            f.writerow([x["phoneNumber"], x["label"], dateutil.parser.parse(x["createdAt"]).strftime("%d-%b-%Y, %H:%M:%S")])
            #f.writerow(x.values())
        print " CSV Generated... \n"

    result = {
        'filePath': csv_file_path,
        'fileName': csv_file_name
    }
    return result

def returnOnlyPath():
    csv_file_name = "WhoCalled_" + datetime.datetime.now().strftime("%B%d") + ".csv"
    csv_file_path = os.path.join(files_dir_path, csv_file_name)
    result = {
        'filePath': csv_file_path,
        'fileName': csv_file_name
    }
    return result
