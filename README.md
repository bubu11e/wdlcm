# wdlcm
A simple tool to manage Warp10 data Life Cycle 

# Installation
    pip3 install wdlcm

# Configuration
wdlcm use by default a configuration file with INI structure. A DEFAULT section is set to use the standard standalone plateform on localhost.
You can add as many cell as you want

Sample:
-------------------------
    [DEFAULT]
    find_endpoint =   http://127.0.0.1:8080/api/v0/find,
    fetch_endpoint =  http://127.0.0.1:8080/api/v0/fetch,
    update_endpoint = http://127.0.0.1:8080/api/v0/update,
    delete_endpoint = http://127.0.0.1:8080/api/v0/delete,
    meta_endpoint =   http://127.0.0.1:8080/api/v0/meta

    [toto]
    read_token  = <read_token for application toto on default cell>
    write_token = <write_token for application toto on default cell>

    [titi]
    find_endpoint =   http://carl.bubu11e.xyz:8080/api/v0/find,
    fetch_endpoint =  http://carl.bubu11e.xyz:8080/api/v0/fetch,
    update_endpoint = http://carl.bubu11e.xyz:8080/api/v0/update,
    delete_endpoint = http://carl.bubu11e.xyz:8080/api/v0/delete,
    meta_endpoint =   http://carl.bubu11e.xyz:8080/api/v0/meta
    read_token  = <read_token for application titi on cell carl>
    write_token = <write_token for application titi on cell carl>

-------------------------

# Commandes

    find <application> <selector>
    delete_all <application> <selector>
    delete_older <application> <selector> <instant>
    mark_empty <application> <selector>
    delete_empty <application>

# Exemples

We assume we use the previous configuration file

    cat << EOF | wdlcm
    delete_all toto ~titi.*{}
    delete_older toto ~.*{} 0
    mark_empty titi ~.*{}
    delete_empty titi
    EOF

This script will delete all the series starting with titi in the application toto on the defaut platform, All the series from the application toto older than 01/01/1970 and finally delete empty series from the titi application on the cell carl.
