import os
import os.path

import tvmauth


TVMAPI_PORT_FILE = "tvmapi.port"
TVMTOOL_PORT_FILE = "tvmtool.port"
TVMTOOL_AUTHTOKEN_FILE = "tvmtool.authtoken"


def _get_tvmapi_port():
    with open(TVMAPI_PORT_FILE) as f:
        return int(f.read())


def _get_tvmtool_params():
    tvmtool_port = int(open(TVMTOOL_PORT_FILE).read())
    authtoken = open(TVMTOOL_AUTHTOKEN_FILE).read()
    return tvmtool_port, authtoken


def test_tvmapi():
    assert os.path.isfile(TVMAPI_PORT_FILE)
    assert os.path.isfile(TVMTOOL_PORT_FILE)
    assert os.path.isfile(TVMTOOL_AUTHTOKEN_FILE)

    ca = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=1000501,
            self_secret='bAicxJVa5uVY7MjDlapthw',
            disk_cache_dir="./",
            dsts={'my backend': 1000502},
            localhost_port=_get_tvmapi_port(),
        )
    )
    assert ca.status == tvmauth.TvmClientStatus.Ok

    tvmtool_port, authtoken = _get_tvmtool_params()
    ct = tvmauth.TvmClient(tvmauth.TvmToolClientSettings("me", auth_token=authtoken, port=tvmtool_port))
    assert ct.status == tvmauth.TvmClientStatus.Ok

    st = ct.check_service_ticket(ca.get_service_ticket_for('my backend'))
    assert st.src == 1000501

    expected_role = '/role/service/auth_type/without_user/access_type/read/handlers/routes/'
    assert expected_role in ct.get_roles().get_service_roles(st)

    ct.stop()
    ca.stop()
