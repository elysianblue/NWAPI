#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

"""\
  NetWitnessHandler.py: 
  
  Python module for querying the NetWitness database directly via the RESTful API.

"""

__author__ = "Wes Riley"
__contact__ = "elysian.blue@gmail.com"
__version__ = "0.2.202308152021"
__maintainer__ = "Wes Riley"
__email__ = "elysian.blue@gmail.com"
__status__ = "Development"


from pyspark.sql import SparkSession
import requests
from requests.auth import HTTPBasicAuth
import json
import yaml
import io
from pyspark.sql.functions import udf, col, explode, collect_list, explode_outer, first, date_format, to_date, from_unixtime, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, TimestampType, MapType
from pyspark.sql import Row
from pyspark.sql import functions as F
from time import strftime, localtime, time



class SparkHandler:

    def __init__(self, config, debug=0):
        self.headers = { 
            'content-type': 'application/json'
        }
        self.config = self.readConfig(config)
        if self.config['netwitness']['settings']['ssl'] == 'enabled':
            self.url = f"https://{self.config['netwitness']['settings']['host']}:{self.config['netwitness']['settings']['port']}/{self.config['netwitness']['settings']['path']}"
        else:
            self.url = f"http://{self.config['netwitness']['settings']['host']}:{self.config['netwitness']['settings']['port']}/{self.config['netwitness']['settings']['path']}"
        self.rest_udf = udf(SparkHandler.executeRestApi, ArrayType(MapType(StringType(), StringType())))

    # Read nwhandler_config.yaml config file and return parsed object
    # * Reads provided YAML config file and loads into parsed object to return
    # readConfig
    # @param confloc Full path location of nwhandler_config.yaml file
    def readConfig(self, confloc):
      try:
          with io.open(confloc, 'r') as configfile:
              config = yaml.safe_load(configfile)
          return config
      except Exception as e:
          print('SparkHandler::readConfig() Exception => ' + str(e) + '\n')

    def sessionQuery(self, verb, url, query):
        res = None
        rec = {}
        rec_list = []
        res_list = []
        query_args = { 'msg': 'query', 'query': query, 'id1': 0, 'id2': 0, 'force-content-type': 'application/json' }
        try:
            if verb == 'get':
                res = requests.get(url, params=query_args, auth=HTTPBasicAuth('admin', 'netwitness'), verify=False)
            elif verb == 'post':
                res = requests.post(url, params=query_args, auth=HTTPBasicAuth('admin', 'netwitness'), verify=False)
            else:
                print('Only get and post supported.')
        except Exception as e:
            return e
        
        if res != None and res.status_code == 200:
            #res_tot = json.loads(res.text)
            #return res_tot[0]['results']['fields']
            for i in res.json():
                rec = {}
                for j in i['results']['fields']:
                    #rec = { 'group': j['group'], 'type': j['type'], 'value': j['value'] }
                    rec.update({ j['type']: j['value'] })
                    rec_list.append(rec)
                    rec = {}
                #print(rec_list)
                #res_list.extend(rec_list)
                #rec_list = []
            #print(res_list)
                    
            return rec_list
            #return res.json()
        
        return None

    @staticmethod
    def executeRestApi(verb, url, query):
        res = None
        rec = {}
        rec_list = {}
        res_list = []
        query_args = { 'msg': 'query', 'query': query, 'id1': 0, 'id2': 0, 'force-content-type': 'application/json' }
        try:
            if verb == 'get':
                res = requests.get(url, params=query_args, auth=HTTPBasicAuth('admin', 'netwitness'), verify=False)
            elif verb == 'post':
                res = requests.post(url, params=query_args, auth=HTTPBasicAuth('admin', 'netwitness'), verify=False)
            else:
                print('Only get and post supported.')
        except Exception as e:
            return e
        
        if res != None and res.status_code == 200:
            #res_tot = json.loads(res.text)
            #return res_tot[0]['results']['fields']
            for i in res.json():
                #rec = {}
                for j in i['results']['fields']:
                    #rec = { 'group': j['group'], 'type': j['type'], 'value': j['value'] }
                    #rec.update({ j['type']: j['value'] })
                    rec[j['type']] = j['value']
                rec_list.update(rec)
                    #rec = {}
                #print(rec_list)
                #res_list.extend(rec_list)
                #rec_list = []
            #print(res_list)
                    
            return Row(**rec_list)
            #return rec
            #return res.json()
        
        return None

    def startSparkSession(self, appName="NWAPI", sparkMaster="spark://172.30.30.69:7077"):
        self.spark = SparkSession.builder \
            .appName(appName) \
            .master(sparkMaster) \
            .enableHiveSupport() \
            .getOrCreate()
        
        

    def createQueryArg(self, query):
        query = "select sessionid where direction=\"outbound\" && service=80 && action=\"post\" && extension=\"php\" && time=\"2020-jan-01 00:00:00\"-u"

        query_args = { 'msg': 'query', 'query': query, 'id1': 0, 'id2': 0, 'force-content-type': 'application/json' }

        response = self.sessionQuery('get', self.url, query)

        return response

    def sessionIdResponseGen(self, response):
        for i in response:
            yield(i['sessionid'])
            #yield(i)

    #sessionIdList = sessionIdResponseGen(response)

    def sessionIdRequestGen(self, response):
        for i in self.sessionIdResponseGen(response):
            query = f"select * where sessionid={i}"
            yield ("get", self.url, query)

    def sparkMetaQuery(self, response):
        RestApiRequestRow = Row("verb", "url", "query")

        request_df = self.spark.createDataFrame(self.sessionIdRequestGen(response), RestApiRequestRow)

        schema = StructType([ \
            StructField('flags', IntegerType(), True), \
            StructField('results', MapType(StringType(), ArrayType(MapType(StringType(), StringType())))) \
        ])

        #bcast_udf = self.executeRestApi
        #udf_executeRestApi = udf(bcast_udf,ArrayType(MapType(StringType(), StringType())))

        result_df = request_df \
            .withColumn('result', self.rest_udf(col('verb'), col('url'), col('query')))
        
        return result_df
    
    
    def sparkStop(self):
        self.spark.stop()

    #print(result_df.count())
    #print(result_df.head())

    #test = result_df.select(col('result').alias('j')).rdd.map(lambda x: x.j)

    def formatMetaResults(self, result_df):
        test_df = result_df.select(explode(result_df.result))
        print(test_df.show(1, truncate=False))
        #test2_df = test_df.select(explode(test_df.col))
        #print(test2_df.show(10))
        test2_df = test_df.select(explode(test_df.col))
        print(test2_df.show(1, truncate=False))
        #test3_df = test2_df.withColumn('record', test2_df.col['fields'])
        #print(test3_df.show(10))
        new_res_df = test_df.select(test_df.col['group'].alias('group'), test_df.col['type'].alias('type'), test_df.col['value'].alias('value')).groupBy('group').pivot('type').agg(collect_list('value'))
        print(new_res_df.show(1, truncate=False))

        return new_res_df

    def make_row(self, kv):
        key, val = kv
        out = dict([('group', key)] + list(val))
        return Row(**out)

    ## RDD Solution
    def formatMetaResultsRDD(self, input_rows):
        #input_rows = []
        #for i in meta["results"]["fields"]:
        #    input_rows.append(i)

        nwRDD = self.spark.sparkContext.parallelize(input_rows)

        nw_test_df = self.spark.read.json(nwRDD)

        nw_test_df.groupBy('type').count().show()

        nw_test_df2 = nw_test_df.groupBy('group').pivot('type').agg(collect_list('value'))

        add_col_DF = nw_test_df.rdd.map(lambda row: (row.group, (row.type, row.value))).groupByKey()

        

        add_col_DF.map(self.make_row).collect()

        pivot_rdd_DF = self.spark.createDataFrame(add_col_DF.map(self.make_row))

        return pivot_rdd_DF
    

    ## Pivot Solution
    def formatMetaResultsPivot(self, nw_test_df):
        df3 = nw_test_df.select("group","type","value").groupBy('group').pivot('type').agg(first('value'))

        new_cols=(column.replace('.','_') for column in df3.columns)

        df3 = df3.toDF(*new_cols)

        df4 = df3.withColumn("ts", from_unixtime(col("time")))

        return df4


# main
# Command-line driver method when used as utility rather than module
def main():
    parser = argparse.ArgumentParser(
       prog='SparkHandler.py',
       description='Middleware library for querying the NetWitness RESTful API via Apache Spark',
       epilog=''
       )
    parser.add_argument('-s', '--size', help='Number of aggregated results to return', metavar='<size>', type=int, default=20)
    parser.add_argument('-f', '--fields', help='Meta Fields to Return as Aggregate Query Result', metavar='<meta field>', nargs='*', required=False)
    parser.add_argument('-w', '--where', help='WHERE Clause for Aggregate Query Filter (single quoted, with values double quoted as necessary - ex: \'action="createprocess" && alias.host="testhost1"\'', metavar='<meta=value OR || OR &&>', default='')
    parser.add_argument('-q', '--query', help='Full NWDB Query String', metavar='<query>')
    args = parser.parse_args()

    nwdb = SparkHandler("nwhandler_config.yaml")

    debug = 0

    if args.query:
        if debug:
              print(f"query: {args.query}")
        nwdb.startSparkSession()
        sessionList = nwdb.createQueryArg(args.query)
        #print(sessionList[0])
        print(sessionList)
        print(type(sessionList))
        print(len(sessionList))
        #for i in sessionList:
        #    print(len(i))
        #    print(type(i))
        #    print(i.keys())
        #sList = []
        #for i in sessionList:
        #    sList.extend(i['value'])
        #print(sList)
        resList_df = nwdb.sparkMetaQuery(sessionList)
        print(resList_df.show(1, truncate=False))
        metaResults = nwdb.formatMetaResults(resList_df)
        print(metaResults.show(1, truncate=False))
        #pivotResults = nwdb.formatMetaResultsPivot(resList_df)
        #print(pivotResults.show(10))
        nwdb.sparkStop()
        
    elif args.size and args.fields and args.where:
       if debug:
          print(f"size:{args.size}")
          print(f"field: {args.fields}")
          print(f"where: {args.where}")
       print(json.dumps(nwdb.queryNWDBAggregate(args.where, args.size, args.fields), indent=3))
    else:
        print(f"No query provided. Use '-q' or '--query' option followed by a NWDB query.")

if __name__ == '__main__':
    import sys
    import argparse
    from SparkHandler import SparkHandler
    main()