#! /usr/bin/env python3 
# -*- coding: utf-8 -*-

__author__ = "Wes Riley"
__contact__ = "elysian.blue@gmail.com"
__version__ = "0.2.202308152021"
__maintainer__ = "Wes Riley"
__email__ = "elysian.blue@gmail.com"
__status__ = "Development"

from flask import Flask, render_template, jsonify, request
from flask_restx import Api, Resource, reqparse, fields, marshal
import NetWitnessHandler.NetWitnessHandler as NWHandler
import time
import sys
import json
import networkx as nx
import requests
from requests.auth import HTTPBasicAuth

debug = 0

app = Flask(__name__)
api = Api(app, version="v0.2.202308152021", title="NetWitness REST API Middleware",
          description="REST API middleware interface layer to NetWitness Endpoints, Packets, and Logs Database")

parser = reqparse.RequestParser()
parser.add_argument('query', help='Netwitness Query String')
parser.add_argument('host')
parser.add_argument('query', location='json')

nwdbQuery = api.model('nwdbQuery', {
    'query': fields.String(required=True),
    'records': fields.Integer(required=False, default=1000)
})

nwdb = NWHandler.NWHandler('./NetWitnessHandler/nwhandler_config.yaml', debug)

@api.route('/api/queryNWDB')
class QueryNWDB(Resource):
    def get(self):
        ret = {'source': '[rest-server.py] QueryNWDB:get()',
               'message': 'Query Endpoint (for testing, since Packets and Endpoint in diff deployments) /api/queryNWDB GET functioning.'}
        return jsonify(ret)

    #@api.doc(params={'data': 'Query string to relay to nwdb', 'records': 'Number of records to return (defaults to 1000)'})
    @api.doc(body=nwdbQuery)
    def post(self):
        resData = request.get_json(force=True)
        if debug:
            print(resData)
        if not resData:
            response = "{ \"Error\": \"No value for \"query\" parameter.\" }"
            return response

        response = jsonify(nwdb.NWGenerate(resData['query']))
        if debug:
            print(json.loads(response.get_data()))
            print(str(type(response.get_data())))
        return json.dumps(response.get_json())
    
@api.route('/api/queryNWDBAggregate')
class QueryNWDBAggregate(Resource):
    def post(self):
        reqData = request.get_json()
        if not reqData:
            response = { "Error": "No data in request." }
            return response
        if debug:
            print(reqData)
            print(request.json)
            print(request)
        response = jsonify(nwdb.queryNWDBAggregate(
            reqData['query'], reqData['size'], reqData['field']))
        return response
    
@app.route('/app')
def frontEnd():
    return render_template('index.html', flask_token='nwrest-api')

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)