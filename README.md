# wdlcm
A simple tool to manage Warp10 data Life Cycle 

# Installation
    pip3 install wdlcm

# Configuration
wdlcm use by default a configuration file with INI structure. A DEFAULT section is set to use the standard standalone plateform on localhost.
You can add ass many cell as you want

Sample:
-------------------------
    [DEFAULT]
    find_endpoint =   http://127.0.0.1:8080/api/v0/find,
    fetch_endpoint =  http://127.0.0.1:8080/api/v0/fetch,
    update_endpoint = http://127.0.0.1:8080/api/v0/update,
    delete_endpoint = http://127.0.0.1:8080/api/v0/delete,
    meta_endpoint =   http://127.0.0.1:8080/api/v0/meta

    [carl]
    find_endpoint =   http://carl.bubu11e.xyz:8080/api/v0/find,
    fetch_endpoint =  http://carl.bubu11e.xyz:8080/api/v0/fetch,
    update_endpoint = http://carl.bubu11e.xyz:8080/api/v0/update,
    delete_endpoint = http://carl.bubu11e.xyz:8080/api/v0/delete,
    meta_endpoint =   http://carl.bubu11e.xyz:8080/api/v0/meta

-------------------------

# Commandes

    delete_all <cell> <selector> <write_token>
    delete_older <cell> <selector> <write_token> <instant>
    mark_empty <cell> <selector> <read_token> <write_token>
    delete_empty <cell> <read_token> <write_token>
