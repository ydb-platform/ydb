from __future__ import print_function

import os

import tvmauth
import tvmauth.unittest

from tvmauth.exceptions import TicketParsingException

import pytest


def _get_port(filename):
    assert os.path.isfile(filename)

    with open(filename) as f:
        return int(f.read())


def get_tvmtool_params():
    return _get_port("tvmtool.port"), open("tvmtool.authtoken").read()


def get_tvmapi_port():
    return _get_port("tvmapi.port")


def get_tirole_port():
    return _get_port("tirole.port")


def create_client_with_tirole(check_src_by_default=None, check_default_uid_by_default=None):
    args = {
        "self_tvm_id": 1000502,
        "self_secret": "e5kL0vM3nP-nPf-388Hi6Q",
        "disk_cache_dir": "./",
        "fetch_roles_for_idm_system_slug": "some_slug_2",
        "enable_service_ticket_checking": True,
        "enable_user_ticket_checking": tvmauth.BlackboxEnv.ProdYateam,
        "localhost_port": get_tvmapi_port(),
        "tirole_host": "http://localhost",
        "tirole_port": get_tirole_port(),
        "tirole_tvmid": 1000001,
    }

    if check_src_by_default is not None:
        args["check_src_by_default"] = check_src_by_default
    if check_default_uid_by_default is not None:
        args["check_default_uid_by_default"] = check_default_uid_by_default

    return tvmauth.TvmClient(tvmauth.TvmApiClientSettings(**args))


def create_client_with_tvmtool(check_src_by_default=None, check_default_uid_by_default=None):
    port, authtoken = get_tvmtool_params()

    args = {
        "self_alias": "me",
        "auth_token": authtoken,
        "port": port,
    }

    if check_src_by_default is not None:
        args["check_src_by_default"] = check_src_by_default
    if check_default_uid_by_default is not None:
        args["check_default_uid_by_default"] = check_default_uid_by_default

    return tvmauth.TvmClient(tvmauth.TvmToolClientSettings(**args))


def check_service_no_roles(clients_with_autocheck=[], clients_without_autocheck=[]):
    # src=1000000000: tvmknife unittest service -s 1000000000 -d 1000502
    st_without_roles = (
        "3:serv:CBAQ__________9_IgoIgJTr3AMQtog9:"
        "Sv3SKuDQ4p-2419PKqc1vo9EC128K6Iv7LKck5SyliJZn5gTAqMDAwb9aYWHhf49HTR-Qmsjw4i_Lh-sNhge-JHWi5PTGFJm03CZHOCJG9Y0_G1pcgTfodtAsvDykMxLhiXGB4N84cGhVVqn1pFWz6SPmMeKUPulTt7qH1ifVtQ"
    )

    for cl in clients_with_autocheck:
        with pytest.raises(TicketParsingException):
            cl.check_service_ticket(st_without_roles)

    for cl in clients_without_autocheck:
        checked = cl.check_service_ticket(st_without_roles)
        assert {} == cl.get_roles().get_service_roles(checked)


def check_service_has_roles(clients_with_autocheck=[], clients_without_autocheck=[]):
    # src=1000000001: tvmknife unittest service -s 1000000001 -d 1000502
    st_with_roles = (
        "3:serv:CBAQ__________9_IgoIgZTr3AMQtog9:"
        "EyPympmoLBM6jyiQLcK8ummNmL5IUAdTvKM1do8ppuEgY6yHfto3s_WAKmP9Pf9EiNqPBe18HR7yKmVS7gvdFJY4gP4Ut51ejS-iBPlsbsApJOYTgodQPhkmjHVKIT0ub0pT3fWHQtapb8uimKpGcO6jCfopFQSVG04Ehj7a0jw"
    )

    def check(cl):
        checked = cl.check_service_ticket(st_with_roles)

        client_roles = cl.get_roles()
        roles = client_roles.get_service_roles(checked)
        assert roles == {
            '/role/service/read/': [{}],
            '/role/service/write/': [
                {
                    'foo': 'bar',
                    'kek': 'lol',
                },
            ],
        }
        assert client_roles.check_service_role(
            checked_ticket=checked,
            role='/role/service/read/',
        )
        assert client_roles.check_service_role(
            checked_ticket=checked,
            role='/role/service/write/',
        )
        assert not client_roles.check_service_role(checked_ticket=checked, role='/role/foo/')

        assert not client_roles.check_service_role(
            checked_ticket=checked,
            role='/role/service/read/',
            exact_entity={'foo': 'bar', 'kek': 'lol'},
        )
        assert not client_roles.check_service_role(
            checked_ticket=checked,
            role='/role/service/write/',
            exact_entity={'kek': 'lol'},
        )
        assert client_roles.check_service_role(
            checked_ticket=checked,
            role='/role/service/write/',
            exact_entity={'foo': 'bar', 'kek': 'lol'},
        )

        with pytest.raises(AttributeError):
            client_roles.check_service_role(
                checked_ticket=checked,
                role='/role/service/read/',
                exact_entity={'foo': 45},
            )

    for cl in clients_with_autocheck:
        check(cl)
    for cl in clients_without_autocheck:
        check(cl)


def check_user_no_roles(clients_with_autocheck=[], clients_without_autocheck=[]):
    # default_uid=1000000000: tvmknife unittest user -d 1000000000 --env prod_yateam
    ut = (
        "3:user:CAwQ__________9_GhYKBgiAlOvcAxCAlOvcAyDShdjMBCgC:"
        "LloRDlCZ4vd0IUTOj6MD1mxBPgGhS6EevnnWvHgyXmxc--2CVVkAtNKNZJqCJ6GtDY4nknEnYmWvEu6-MInibD-Uk6saI1DN-2Y3C1Wdsz2SJCq2OYgaqQsrM5PagdyP9PLrftkuV_ZluS_FUYebMXPzjJb0L0ALKByMPkCVWuk"
    )

    for cl in clients_with_autocheck:
        with pytest.raises(TicketParsingException):
            cl.check_user_ticket(ut)

    for cl in clients_without_autocheck:
        checked = cl.check_user_ticket(ut)
        assert {} == cl.get_roles().get_user_roles(checked)


def check_user_has_roles(clients_with_autocheck=[], clients_without_autocheck=[]):
    # default_uid=1120000000000001: tvmknife unittest user -d 1120000000000001 --env prod_yateam
    ut_with_roles = (
        "3:user:CAwQ__________9_GhwKCQiBgJiRpdT-ARCBgJiRpdT-ASDShdjMBCgC:"
        "SQV7Z9hDpZ_F62XGkSF6yr8PoZHezRp0ZxCINf_iAbT2rlEiO6j4UfLjzwn3EnRXkAOJxuAtTDCnHlrzdh3JgSKK7gciwPstdRT5GGTixBoUU9kI_UlxEbfGBX1DfuDsw_GFQ2eCLu4Svq6jC3ynuqQ41D2RKopYL8Bx8PDZKQc"
    )

    def check(cl):
        checked = cl.check_user_ticket(ut_with_roles)

        client_roles = cl.get_roles()
        roles = client_roles.get_user_roles(checked)
        assert roles == {
            '/role/user/write/': [{}],
            '/role/user/read/': [
                {
                    'foo': 'bar',
                    'kek': 'lol',
                },
            ],
        }
        assert client_roles.check_user_role(
            checked_ticket=checked,
            role='/role/user/write/',
        )
        assert client_roles.check_user_role(
            checked_ticket=checked,
            role='/role/user/read/',
        )
        assert not client_roles.check_user_role(checked_ticket=checked, role='/role/foo/')

        assert not client_roles.check_user_role(
            checked_ticket=checked,
            role='/role/user/write/',
            exact_entity={'foo': 'bar', 'kek': 'lol'},
        )
        assert not client_roles.check_user_role(
            checked_ticket=checked,
            role='/role/user/read/',
            exact_entity={'kek': 'lol'},
        )
        assert client_roles.check_user_role(
            checked_ticket=checked,
            role='/role/user/read/',
            exact_entity={'foo': 'bar', 'kek': 'lol'},
        )

        with pytest.raises(AttributeError):
            client_roles.check_user_role(
                checked_ticket=checked,
                role='/role/user/read/',
                exact_entity={'foo': 45},
            )

    for cl in clients_with_autocheck:
        check(cl)
    for cl in clients_without_autocheck:
        check(cl)


def test_roles_from_tirole_check_src__no_roles():
    client_with_autocheck1 = create_client_with_tirole(check_src_by_default=None)
    client_with_autocheck2 = create_client_with_tirole(check_src_by_default=True)
    client_without_autocheck = create_client_with_tirole(check_src_by_default=False)

    check_service_no_roles(
        clients_with_autocheck=[client_with_autocheck1, client_with_autocheck2],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck1.stop()
    client_with_autocheck2.stop()
    client_without_autocheck.stop()


def test_roles_from_tirole_check_src__has_roles():
    client_with_autocheck = create_client_with_tirole(check_src_by_default=True)
    client_without_autocheck = create_client_with_tirole(check_src_by_default=False)

    check_service_has_roles(
        clients_with_autocheck=[client_with_autocheck],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck.stop()
    client_without_autocheck.stop()


def test_roles_from_tirole_check_default_uid__no_roles():
    client_with_autocheck1 = create_client_with_tirole(check_default_uid_by_default=None)
    client_with_autocheck2 = create_client_with_tirole(check_default_uid_by_default=True)
    client_without_autocheck = create_client_with_tirole(check_default_uid_by_default=False)

    check_user_no_roles(
        clients_with_autocheck=[client_with_autocheck1, client_with_autocheck2],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck1.stop()
    client_with_autocheck2.stop()
    client_without_autocheck.stop()


def test_roles_from_tirole_check_default_uid__has_roles():
    client_with_autocheck = create_client_with_tirole(check_default_uid_by_default=True)
    client_without_autocheck = create_client_with_tirole(check_default_uid_by_default=False)

    check_user_has_roles(
        clients_with_autocheck=[client_with_autocheck],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck.stop()
    client_without_autocheck.stop()


def test_roles_from_tvmtool_check_src__no_roles():
    client_with_autocheck1 = create_client_with_tvmtool(check_src_by_default=None)
    client_with_autocheck2 = create_client_with_tvmtool(check_src_by_default=True)
    client_without_autocheck = create_client_with_tvmtool(check_src_by_default=False)

    check_service_no_roles(
        clients_with_autocheck=[client_with_autocheck1, client_with_autocheck2],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck1.stop()
    client_with_autocheck2.stop()
    client_without_autocheck.stop()


def test_roles_from_tvmtool_check_src__has_roles():
    client_with_autocheck = create_client_with_tvmtool(check_src_by_default=True)
    client_without_autocheck = create_client_with_tvmtool(check_src_by_default=False)

    check_service_has_roles(
        clients_with_autocheck=[client_with_autocheck],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck.stop()
    client_without_autocheck.stop()


def test_roles_from_tvmtool_check_default_uid__no_roles():
    client_with_autocheck1 = create_client_with_tvmtool(check_default_uid_by_default=None)
    client_with_autocheck2 = create_client_with_tvmtool(check_default_uid_by_default=True)
    client_without_autocheck = create_client_with_tvmtool(check_default_uid_by_default=False)

    check_user_no_roles(
        clients_with_autocheck=[client_with_autocheck1, client_with_autocheck2],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck1.stop()
    client_with_autocheck2.stop()
    client_without_autocheck.stop()


def test_roles_from_tvmtool_check_default_uid__has_roles():
    client_with_autocheck = create_client_with_tvmtool(check_default_uid_by_default=True)
    client_without_autocheck = create_client_with_tvmtool(check_default_uid_by_default=False)

    check_user_has_roles(
        clients_with_autocheck=[client_with_autocheck],
        clients_without_autocheck=[client_without_autocheck],
    )

    client_with_autocheck.stop()
    client_without_autocheck.stop()
