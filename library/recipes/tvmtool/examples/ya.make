OWNER(g:passport_infra)

RECURSE_FOR_TESTS(
    ut_simple
    ut_with_custom_config
    ut_with_roles
    ut_with_tvmapi
)

IF (NOT SANITIZER_TYPE)
    RECURSE(
        ut_with_tvmapi_and_tirole
    )
ENDIF()
