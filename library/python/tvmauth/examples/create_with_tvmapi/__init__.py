import tvmauth


def get_client_for_checking_all_tickets_and_fetching_service_tickets():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=11,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            self_secret="AAAAAAAAAAAAAAAAAAAAAA",
            dsts={"bb": 224, "datasync": 2000060},
            disk_cache_dir='/var/lib/foo/tvm_cache/',
        )
    )

    # c.check_service_ticket("some service ticket")
    # c.check_user_ticket("some user ticket")
    # c.get_service_ticket_for("bb")
    # c.get_service_ticket_for(tvm_id=224)

    return c


def get_client_for_checking_all_tickets():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=11,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            disk_cache_dir='/var/lib/foo/tvm_cache/',
        )
    )

    # c.check_service_ticket("some service ticket")
    # c.check_user_ticket("some user ticket")

    return c


def get_client_for_fetching_service_tickets():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=11,
            self_secret="AAAAAAAAAAAAAAAAAAAAAA",
            dsts={"bb": 224, "datasync": 2000060},
            disk_cache_dir='/var/lib/foo/tvm_cache/',
        )
    )

    # c.get_service_ticket_for("bb")
    # c.get_service_ticket_for(tvm_id=224)

    return c


def get_client_for_checking_service_tickets():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=11,
            enable_service_ticket_checking=True,
            disk_cache_dir='/var/lib/foo/tvm_cache/',
        )
    )

    # c.check_service_ticket("some service ticket")

    return c


def get_client_for_checking_user_tickets():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            disk_cache_dir='/var/lib/foo/tvm_cache/',
        )
    )

    # c.check_user_ticket("some user ticket")

    return c


def get_client_for_checking_all_tickets_with_roles():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=11,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tvmauth.BlackboxEnv.ProdYateam,
            self_secret="AAAAAAAAAAAAAAAAAAAAAA",
            disk_cache_dir='/var/lib/foo/tvm_cache/',
            fetch_roles_for_idm_system_slug='passporttestservice',
        )
    )

    # t = c.check_user_ticket("some user ticket")
    # c.get_roles().check_user_role(t, "some role")

    return c


def get_client_for_checking_service_tickets_with_roles():
    c = tvmauth.TvmClient(
        tvmauth.TvmApiClientSettings(
            self_tvm_id=11,
            enable_service_ticket_checking=True,
            self_secret="AAAAAAAAAAAAAAAAAAAAAA",
            disk_cache_dir='/var/lib/foo/tvm_cache/',
            fetch_roles_for_idm_system_slug='passporttestservice',
        )
    )

    # t = c.check_service_ticket("some service ticket")
    # c.get_roles().check_service_role(t, "some role")

    return c
