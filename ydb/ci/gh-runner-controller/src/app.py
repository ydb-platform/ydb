import argparse
import json
import logging
import logging.config
import signal
from threading import Event


from scaler.config import Config
from scaler.ch import Clickhouse
from scaler.controller import ScaleController
from scaler.gh import Github
from scaler.yc import YandexCloudProvider


def prepare_logger():
    # FIXME: fix very long name like kikimr.scripts.oss.github_runner_scale.scaler.lib.controller
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "standard": {"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"},
            },
            "handlers": {
                "default": {
                    #
                    "level": "NOTSET",
                    "formatter": "standard",
                    "class": "logging.StreamHandler",
                },
            },
            "loggers": {
                '': {
                    #
                    "handlers": ["default"],
                    "level": "INFO",
                    "propagate": False,
                },
                "scaler": {
                    #
                    "handlers": ["default"],
                    "level": "DEBUG",
                    "propagate": False,
                },
                "__main__": {
                    # if __name__ == '__main__'
                    "handlers": ["default"],
                    "level": "DEBUG",
                    "propagate": False,
                },
            },
        }
    )


def main():
    prepare_logger()
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument("conf", help="yaml app config")

    args = parser.parse_args()

    cfg = Config.load(args.conf)

    yc_auth_sa_key = None

    if cfg.yc_auth_sa_key:
        with open(cfg.yc_auth_sa_key, "r") as fp:
            yc_auth_sa_key = json.load(fp)

    yc = YandexCloudProvider(cfg.yc_auth_use_metadata, yc_auth_sa_key)

    cfg.late_discover_from_yc(yc)

    # FIXME: config must be singleton and can be imported from everywhere
    yc.set_config(cfg)

    ch = Clickhouse(
        cfg.clickhouse_fqdns,
        cfg.clickhouse_database,
        cfg.clickhouse_username,
        cfg.clickhouse_password,
    )
    gh = Github(cfg.gh_repo, cfg.gh_token)

    exit_event = Event()

    # noinspection PyUnusedLocal
    def sigint_handler(signum, frame):
        logger.info("catch SIGINT, set exit_event")
        # FIXME: remove
        raise SystemExit(0)
        exit_event.set()

    signal.signal(signal.SIGINT, sigint_handler)
    ctrl = ScaleController(cfg, ch, gh, yc, exit_event)
    ctrl.loop()


if __name__ == "__main__":
    main()
