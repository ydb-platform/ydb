import os
import os.path

import ticket_parser2 as tp2


TVMAPI_PORT_FILE = "tvmapi.port"
TVMTOOL_PORT_FILE = "tvmtool.port"
TVMTOOL_AUTHTOKEN_FILE = "tvmtool.authtoken"


def _get_tvmapi_port():
    with open(TVMAPI_PORT_FILE) as f:
        return int(f.read())


def _get_tvmtool_params():
    port = int(open(TVMTOOL_PORT_FILE).read())
    authtoken = open(TVMTOOL_AUTHTOKEN_FILE).read()
    return port, authtoken


def test_tvmapi():
    assert os.path.isfile(TVMAPI_PORT_FILE)
    assert os.path.isfile(TVMTOOL_PORT_FILE)
    assert os.path.isfile(TVMTOOL_AUTHTOKEN_FILE)

    port = _get_tvmapi_port()

    cs = tp2.TvmApiClientSettings(
        self_client_id=1000501,
        self_secret='bAicxJVa5uVY7MjDlapthw',
        dsts={'my backend': 1000502},
        enable_service_ticket_checking=True,
    )
    cs.__set_localhost(port)

    ca = tp2.TvmClient(cs)
    assert ca.status == tp2.TvmClientStatus.Ok

    port, authtoken = _get_tvmtool_params()
    ct = tp2.TvmClient(tp2.TvmToolClientSettings("me", auth_token=authtoken, port=port))
    assert ct.status == tp2.TvmClientStatus.Ok

    st = ca.check_service_ticket(ct.get_service_ticket_for(client_id=1000501))
    assert st.src == 1000503

    ct.stop()
    ca.stop()
