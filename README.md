# NWAPI
Scripts for interacting with NetWitness API

## NetWitnessHandler
### NetWitnessHandler.py
- Query NWDB via Restful API and convert results to session objects
- Query NWDB to aggregate meta fields given WHERE condition

### nwhandler_config.yaml
- YAML config file containing NetWitness host, SDK port, SSL config, and credential information

## nwrest-api.py
- Basic Flask REST API app with endpoints mapped to the NWDB query methods provided in NetWitnessHandler.py