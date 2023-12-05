PY3_LIBRARY()

PY_SRCS(
    ch.py
    cloud_config.py
    config.py
    controller.py
    gh.py
    jobs.py
    utils.py
    yc.py
)

PEERDIR(
    contrib/python/requests/py3
    contrib/python/yandexcloud
    contrib/python/PyYAML/py3
)

RESOURCE_FILES(
    PREFIX kikimr/scripts/oss/github_runner_scale/scaler/lib/
    scripts/install_runner.sh
)

END()
