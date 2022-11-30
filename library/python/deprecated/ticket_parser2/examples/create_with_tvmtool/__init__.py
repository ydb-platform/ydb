import ticket_parser2 as tp2


# Possibility of using functions depends on config of tvmtool
#    check_service_ticket
#    check_user_ticket
#    get_service_ticket_for


def get_client_for_dev():
    c = tp2.TvmClient(
        tp2.TvmToolClientSettings(
            self_alias="me",
            auth_token="AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            port=18080,
        )
    )

    return c


def get_client_in_qloud_or_yandexdeploy():
    c = tp2.TvmClient(
        tp2.TvmToolClientSettings(
            self_alias="me",
        )
    )

    return c
