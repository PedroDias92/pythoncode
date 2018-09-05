from pymongo import MongoClient
from datetime import datetime
import argparse
import csv
import json
import dateutil.parser
from bson import json_util, ObjectId
from pandas.io.json import json_normalize
import json
import pandas as pd
import ast
import pprint
import re
import re
import datetime
import dateutil.parser
from bson.objectid import ObjectId


'''
OK!
python3.6 mongoexport.py -host XXXXX  --port XXXX --database product --username readonly --password XXXXXX --collection XXXXXX --query '{"origin":"XXXX.XXXX"}'

OK
python3.6 mongoexport.py -host XXXXX  --port XXXXX --database product --username readonly --password XXXXX --collection products --query '[{"$match":{"origin":"XXXXX","status":"ACTIVE","created_at": {"$gte": ISODate("2018-07-13T17:04:11.668Z"),"$lte": ISODate("2018-07-16T17:04:11.668Z")}}},{"$unwind":"$barcodes"},{"$limit":5}]' -a --output out --type csv



'''

def datetime_parser(json_dict):
    for (key, value) in json_dict.items():
        try:
            json_dict[key] = dateutil.parser.parse(value)
        except (ValueError, AttributeError):
            pass
    return json_dict


def to_csv(cursor,filename,delimiter,encoding):
    sanitized = json.loads(json_util.dumps(cursor))
    #print(sanitized)
    normalized = json_normalize(sanitized,sep="_")
    #print(normalized.keys())
    df = pd.DataFrame(normalized)
    #print(df)
    df.to_csv(filename+'.csv', index=False,sep=delimiter,encoding=encoding)

def to_json(cursor,filename):
    sanitized = json.loads(json_util.dumps(cursor))
    #print(sanitized)
    normalized = json_normalize(sanitized,sep="_")
    #print(normalized.keys())
    df = pd.DataFrame(normalized)
    #print(df)
    df.to_json(filename+'.json',orient = "records")

'''goes to each dictonary inside another and change the isodate to datetime.datetime'''
def transformDate(jdata):
    #print("NOVO\t",jdata)
    for d in jdata:
        for (key,value) in d.items():
            #print(isinstance(value,dict))
            if isinstance(value,dict):
                transformDate([value])       
            try:
                d[key]=dateutil.parser.parse(value,ignoretz=True)
            except:
                pass
    return jdata



def main():

    parser = argparse.ArgumentParser(description = 'Mongo Export', usage = 'mongoexport [options]')
    parser.add_argument('-host', '--hostname', metavar = '', required = True, help = 'Mongo hostname')
    parser.add_argument('-p', '--port', type = int, metavar = '', required = True, help = 'Mongo port')
    parser.add_argument('-user', '--username', metavar = '', required = True, help = 'Mongo username')
    parser.add_argument('-pass', '--password', metavar = '', required = True, help = 'Mongo password')
    parser.add_argument('-db', '--database', metavar = '', required = True, help = 'Mongo database')
    parser.add_argument('-col', '--collection', metavar = '', required = True, help = 'Mongo collection')
    parser.add_argument('-q', '--query', metavar = '', default = '{}', help = "Mongo query to extract, for aggregation use '[{\"$match\": ... }]'")
    parser.add_argument('-a', '--aggregate', action = 'store_true', help = 'Aggregation flag')
    #parser.add_argument('-pip', '--pipeline', metavar = '', help = 'Aggregate pipeline to extract')
    parser.add_argument('-out', '--output', metavar = '', default = 'mongoexport', help = 'Output prefix filename. Default is mongoexport')
    parser.add_argument('-t', '--type',  metavar = '', default = 'csv', help = 'Output file type (csv or json). Default is csv')
    parser.add_argument('-e', '--encoding', metavar = '', default = 'utf-8', help = 'Output file encoding. Default is utf-8')
    parser.add_argument('-d', '--delimiter', metavar = '', default = ';', help = 'Output file delimiter. Default is ";"')

    args = parser.parse_args()

    mongo_uri = 'mongodb://' + args.username + ':' + args.password + '@' + args.hostname + ':' + str(args.port)
    client = MongoClient(mongo_uri)

    database = client.database
    db = client[args.database]
    #collection = database.collection
    collection = db[args.collection]
    

    #para AGGREGATE
    if args.aggregate:
        s = str(args.query).replace("'","")
        #print("ORIGINAL QUERY\n",s)
        s = s.replace("ISODate(","").replace(')','') #retirar o ISODate!!
        #print("query sem ISO  ",s)
        
        jdata = json.loads(s)
        print("query em JSON  ",jdata)

        jdata=transformDate(jdata)

        print("JSON QUERY\n",jdata)
        cursor = collection.aggregate(jdata)
        #for doc in cursor:
        #    pprint.pprint(doc)
        

    else: # para FIND!
        args.query=args.query.replace("ISODate(","").replace(')','') #retirar o ISODate!!  "2018-07-13T17:04:11.668Z"
        print("args  ",args.query)
        query = json.loads(args.query, object_hook=datetime_parser)
        print("query   ",query)
        #print()
        cursor = collection.find(query).limit(3)
    
        #for doc in cursor:
        #    pprint.pprint(doc)
    

    #por alguma razão não imprime para o CSV se imprimir no terminal 1º
    '''to csv'''

    if not(args.type) or args.type == "csv":
        to_csv(cursor,args.output,args.delimiter,args.encoding)
    elif args.type == "json":
        to_json(cursor,args.output)
    


if __name__ == '__main__':
    main()


