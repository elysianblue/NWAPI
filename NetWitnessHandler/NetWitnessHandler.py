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

# Handler Module for NetWitness Database Communication
import json
import requests
from requests.auth import HTTPBasicAuth
from collections import defaultdict
import time
import yaml
import io
import pyodbc
import unicodecsv

class NWHandler:

  # Constructor
  # * Debug set to 1 will activate all the debug print() statements
  # * URL set here for test/dev, will probably store here on exec
  # * URL constructed from YAML config file passed during class instantiation
  def __init__(self, confloc, debug=0):
      self.config = self.readConfig(confloc)
      self.debug = debug
      if self.config['netwitness']['settings']['ssl'] == 'enabled':
          self.url = 'https://' + self.config['netwitness']['settings']['host'] + ':' + self.config['netwitness']['settings']['port'] + '/' + self.config['netwitness']['settings']['path']
      else:
          self.url = 'http://' + self.config['netwitness']['settings']['host'] + ':' + self.config['netwitness']['settings']['port'] + '/' + self.config['netwitness']['settings']['path']
   
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
          print('NWHandler::readConfig() Exception => ' + str(e) + '\n')

  # * Validation Function Section
  # validVar
  # @param testVar This is the parameter to be tested 
  def validVar(self, testVar):
      if testVar:
          return True
      else:
          return False

  # safe_str
  # @param obj Identifies special non-ascii cases of strings and converts them accordingly
  def safe_str(self, obj):
      try:
          return str(obj)
      except UnicodeEncodeError:
          return unicode(obj).encode('unicode_escape')
      except UnicodeDecodeError:
          return unicode(obj).encode('latin-1')

    
  # * Single Query Function Section (Current/Common Use Case)
  # NWGenerate
  # Execute NWDB query directly against NWDB and parse the results to group by group identifier (effectively session ID). Returns list of dictionaries containing requested session meta in form of metaKey: metaValue.
  # @param query Query to execute against Netwitness NWDB directly
  def NWGenerate(self, query):
      queryArgs = { 'msg': 'query', 'query': query, 'force-content-type': 'application/json' }
      nwResult = requests.get(self.url, params=queryArgs, auth=HTTPBasicAuth(self.config['netwitness']['auth']['user'], self.config['netwitness']['auth']['pass']), verify=False).json()
      resultParsed = []
      recordDict = {}
      cur_group = 0
      if isinstance(nwResult, dict):
          nwResult = [nwResult]

      for result in nwResult:
          for fields in result['results']['fields']:
              if cur_group == 0:
                  cur_group = fields['group']
                  recordDict[fields['type']] = fields['value']
              elif cur_group == fields['group']:
                  recordDict[fields['type']] = fields['value']
              else:
                  cur_group = fields['group']
                  resultParsed.append(recordDict.copy())
                  recordDict[fields['type']] = fields['value']

      if self.debug:
         print(f"NWGenerate: resultParsed length = {str(len(resultParsed))}")
         print(f"NWGenerate: resultParsed type = {str(type(resultParsed))}")

      return resultParsed
  
  # processNetwitnessMeta
  # @param meta JSON marshalled Netwitness HTTP API results in per-metavalue record format
  # @param sessions sessions Empty list for storing processed session/event records, using Python's default pass-by-reference as to not unnecessarily duplicate data in memory
  # @param page_size Page size requested by user. Actual size of returned Netwitness results are page_size * (len(requested_field_list) + 3), as Netwitness counts per-metavalue in its pagination
  def processNetwitnessMeta(self, meta, sessions, page_size=1000):
    """processNetwitnessMeta - Method to convert the Netwitness HTTP API results of per-metavalue records to a per-session/per-event record in keeping with the session/event abstraction
    
    Arguments:
        meta {dict} -- JSON marshalled Netwitness HTTP API results in per-metavalue record format
        sessions {list} -- Empty list for storing processed session/event records, using Python's default pass-by-reference as to not unnecessarily duplicate data in memory
        page_size {int} -- Page size requested by user. Actual size of returned Netwitness results are page_size * (len(requested_field_list) + 3), as Netwitness counts per-metavalue in its pagination
    
    Returns:
        none -- Resulting data is stored in sessions list of calling function via pass-by-reference of that argument
    """
    if self.debug:
      print('NetWitnessHandler - NWHandler:processNetwitnessMeta(): meta type: ' + str(type(meta)) + ', length: ' + str(len(meta)) + ', beginning record processing...')
      startTime = time.time()
      print(meta)
    try:
      d = {}
      rec_count = 0
      i = 0
      current_group = 'NOT_SET'
      window_start_meta_id =  0
      window_end_meta_id = 0

      ## Process each disparate meta field result into coherent session
      for rec in meta:
        i += 1
        if self.debug:
          if i == len(meta):
            sessions.append(d)
            window_end_meta_id = row['id2']
            print("End Meta ID: " + str(row['id2']))
            return

        if 'results' in rec:
          #print(group)
          for row in rec['results']['fields']:
            if rec_count == page_size:
              if self.debug:
                print("NetWitnessHandler - NWHandler:processNetwitnessMeta(): End Meta for Window Tracking: "+ str(window_end_meta_id))
                endTime = time.time()
                print('NetWitnessHandler - NWHandler:processNetwitnessMeta(): Processing time: ' + str(endTime - startTime))
              return
            group = str(row['group'])

            ## Initial condition (first session processing starts)
            if current_group == 'NOT_SET':
              current_group = group
              d = {}
              window_start_meta_id = row['id1']
              if self.debug:
                print("First Meta ID" + str(row['id1']))
              fixedVal = str(row['type']).replace('.', '_')
              d[fixedVal] = row['value']
            ## Intermediate condition (continue processing current session)
            elif (current_group == group):
              fixedVal = str(row['type']).replace('.', '_')
              d[fixedVal] = row['value']
             
            ## End condition (store processed session, create new empty session, inc session counter (rec_count), begin processing new session)
            else:
              current_group = group
              sessions.append(d)
              window_end_meta_id = row['id2']
              rec_count += 1
              d = {}
              fixedVal = str(row['type']).replace('.', '_')
              d[fixedVal] = row['value']

    except Exception as e:
      print(e)


  # queryNWDB
  # Method to query NWDB directly. This method doesn't parse the data returned from NWDB to expected JSON itself, it passes to the processNetwitnessMeta() method to do the parsing.
  # @param query Query to send to NWDB
  # @param records Max number of records to return. This references the full parsed session records, the actual max records returned by NWDB is unconstrained, so will actually pull back more data from NWDB that will return.
  def queryNWDB(self, query, records=1000):
    # Example query: 'select sessionid, event.time, alias.host, user.src, directory.src, filename.src, param.src, action, directory.dst, filename.dst, param.dst, checksum.src, checksum.dst where device.type="nwendpoint" && action exists '
    if self.debug:
       print(query)
    if not query:
      ret = []
      error = { 'error': 'No query provided', 'type': 'input' }
      ret.append(error)
      return ret

    if self.debug:
      print(query)
    query_args = { 'msg': 'query', 'query': query, 'id1': 0, 'id2': 0, 'force-content-type': 'application/json' }
    if self.debug:
      print(query_args)
    startTime = time.time()
    if self.debug:
       print(self.url)
    response = requests.get( self.url, params=query_args, auth=HTTPBasicAuth(self.config['netwitness']['auth']['user'], self.config['netwitness']['auth']['pass']), verify=False)
    endTime = time.time()
    if self.debug:
      print('NetWitnessHandler - NWHandler:queryNWDB(): nwdb query completed in: ' + str(endTime - startTime))
    
    sessions = []
    startTime = time.time()
    meta = json.loads(response.text)
    endTime = time.time()
    if self.debug:
        print('NetWitnessHandler - NWHandler:queryNWDB(): results loaded in: ' + str(endTime - startTime))
    if type(meta) == dict:
      if self.debug:
        print('NetWitnessHandler - NWHandler:queryNWDB(): Response from nwdb is of type: ' + str(type(meta)) + ', inserting to list for some consistency...')
      data = []
      data.append(meta)
      self.processNetwitnessMeta(data, sessions, records)
    elif type(meta) == list:
      if self.debug:
        print('NetWitnessHandler - NWHandler:queryNWDB(): Response from nwdb is of type: ' + str(type(meta)) + ', sending along...')
      self.processNetwitnessMeta(meta, sessions, records)

    return sessions
    

  # * Aggregation Function Section
  # processNetwirntessMetaAggregate
  # @param meta 
  # @param results
  def processNetwitnessMetaAggregate(self, meta, results):
      try:
          d = {}
          rec_count = 0
          current_group = 'NOT_SET'
          window_start_meta_id = 0
          window_end_meta_id = 0
          
          print(meta)
          print('-------------------')
          
          for field in meta['results']['fields']:
              print(field)
              print(results)
              d[field['type']] = field['value']
              d['count'] = field['count']
              results.append(d.copy())

      except Exception as e:
          print('NWHandler:processNetwitnessMetaAggregate(): Exception => ' + e + '\n')

  # queryNWDBAggregate
  # @param query Query to select sessions to aggregate across
  # @param size Records to return
  # @param field Fields to aggregate across
  def queryNWDBAggregate(self, query, size, field):
      translated_size = size * 8
      print('translated_size: ' + str(translated_size) + '\n')
      query_args = { 'msg': 'values', 'size': translated_size, 'fieldName': field, 'where': query, 'force-content-type': 'application/json' }
      print(query_args)
      response = requests.get( self.url, params=query_args, auth=HTTPBasicAuth(self.config['netwitness']['auth']['user'], self.config['netwitness']['auth']['pass']), verify=False )
      results = []
      self.processNetwitnessMetaAggregate(json.loads(response.text), results)
      return results

# main
# Command-line driver method when used as utility rather than module
def main():
    parser = argparse.ArgumentParser(
       prog='NetWitnessHandler.py',
       description='Middleware library for querying the NetWitness RESTful API',
       epilog=''
       )
    parser.add_argument('-s', '--size', help='Number of aggregated results to return', metavar='<size>', type=int, default=20)
    parser.add_argument('-f', '--fields', help='Meta Fields to Return as Aggregate Query Result', metavar='<meta field>', nargs='*', required=False)
    parser.add_argument('-w', '--where', help='WHERE Clause for Aggregate Query Filter (single quoted, with values double quoted as necessary - ex: \'action="createprocess" && alias.host="testhost1"\'', metavar='<meta=value OR || OR &&>', default='')
    parser.add_argument('-q', '--query', help='Full NWDB Query String', metavar='<query>')
    args = parser.parse_args()

    nwdb = NetWitnessHandler.NWHandler("nwhandler_config.yaml")

    debug = 0

    if args.query:
        if debug:
              print(f"query: {args.query}")
        print(json.dumps(nwdb.queryNWDB(args.query), indent=3))
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
    import NetWitnessHandler
    main()