PY3_PROGRAM(mnc_viewer)

    SUBSCRIBER(kruall)

    PY_MAIN(ydb.tools.mnc.viewer.main)

    PY_SRCS(
        commands.py
        main.py
        pages/__init__.py
        pages/cluster_config.py
        pages/hosts.py
        pages/operations.py
        pages/overview.py
        pages/settings.py
        styles/__init__.py
        styles/cluster_config.py
        styles/common.py
        styles/hosts.py
        styles/operations.py
        styles/overview.py
        styles/settings.py
        widgets/__init__.py
        widgets/config_models.py
        widgets/host_card.py
        widgets/modals.py
        widgets/operation_form.py
        widgets/operation_models.py
        widgets/path_picker.py
        widgets/settings_fields.py
        widgets/state.py
    )

    PEERDIR(
        contrib/python/PyYAML
        contrib/python/rich
        contrib/python/textual
        ydb/tools/mnc/cli
        ydb/tools/mnc/lib
        ydb/tools/mnc/scheme
    )

END()

RECURSE_FOR_TESTS(
    ut
)
