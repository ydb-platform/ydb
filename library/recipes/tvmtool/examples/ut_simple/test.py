import os
import os.path
import requests

TVMTOOL_PORT_FILE = "tvmtool.port"
TVMTOOL_AUTHTOKEN_FILE = "tvmtool.authtoken"


def _get_tvmtool_params():
    port = int(open(TVMTOOL_PORT_FILE).read())
    authtoken = open(TVMTOOL_AUTHTOKEN_FILE).read()
    return port, authtoken


def test_tvmtool():
    assert os.path.isfile(TVMTOOL_PORT_FILE)
    assert os.path.isfile(TVMTOOL_AUTHTOKEN_FILE)

    port, authtoken = _get_tvmtool_params()

    r = requests.get("http://localhost:%d/tvm/ping" % port)
    assert r.text == 'OK'
    assert r.status_code == 200

    r = requests.get("http://localhost:%d/tvm/keys" % port, headers={'Authorization': authtoken})
    assert r.status_code == 200
