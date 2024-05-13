# NWAPI
Scripts for interacting with NetWitness API

## NetWitnessHandler
### NetWitnessHandler.py
- Query NWDB via Restful API and convert results to session objects
- Query NWDB to aggregate meta fields given WHERE condition
- Usage Example (Aggregation): 
    - `NetWitnessHandler.py -s 100 -f ip.src -w 'direction="inbound" && service=80 && action="POST" && extension="php"'` 
- Usage Example (Query): 
    - `NetWitnessHandler.py -q 'select ip.src where direction="inbound" && service=80 && action="POST" && extension="php"'`


### nwhandler_config.yaml
- YAML config file containing NetWitness host, SDK port, SSL config, and credential information

## NWREST-API Flask API App
### nwrest-api.py
- Basic Flask REST API app with endpoints mapped to the NWDB query methods provided in NetWitnessHandler.py
- Endpoints:
    - `/api/queryNWDB`
        - Method: `POST`
        - Parameters: 
            - `query`: Full query to submit to NWDB over NetWitness RESTful API
            - `records`: Max number of records to return
    - `/api/queryNWDBAggregate`
        - Method: `POST`
        - Parameters: 
            - `query`: WHERE condition to submit to NWDB over NetWitness RESTful API
            - `size`: Max number of records to return
            - `field`: Meta field on which to aggregate results