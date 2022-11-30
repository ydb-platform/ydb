import ticket_parser2 as tp2


def get_client_for_checking_all_tickets_and_fetching_service_tickets():
    c = tp2.TvmClient(
        tp2.TvmApiClientSettings(
            self_client_id=11,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
            self_secret="AAAAAAAAAAAAAAAAAAAAAA",
            dsts={"bb": 224, "datasync": 2000060},
        )
    )

    # c.check_service_ticket("some service ticket")
    # c.check_user_ticket("some user ticket")
    # c.get_service_ticket_for("bb")
    # c.get_service_ticket_for(client_id=224)

    return c


def get_client_for_checking_all_tickets():
    c = tp2.TvmClient(
        tp2.TvmApiClientSettings(
            self_client_id=11,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
        )
    )

    # c.check_service_ticket("some service ticket")
    # c.check_user_ticket("some user ticket")

    return c


def get_client_for_fetching_service_tickets():
    c = tp2.TvmClient(
        tp2.TvmApiClientSettings(
            self_client_id=11,
            self_secret="AAAAAAAAAAAAAAAAAAAAAA",
            dsts={"bb": 224, "datasync": 2000060},
        )
    )

    # c.get_service_ticket_for("bb")
    # c.get_service_ticket_for(client_id=224)

    return c


def get_client_for_checking_service_tickets():
    c = tp2.TvmClient(
        tp2.TvmApiClientSettings(
            self_client_id=11,
            enable_service_ticket_checking=True,
        )
    )

    # c.check_service_ticket("some service ticket")

    return c


def get_client_for_checking_user_tickets():
    c = tp2.TvmClient(
        tp2.TvmApiClientSettings(
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
        )
    )

    # c.check_user_ticket("some user ticket")

    return c
