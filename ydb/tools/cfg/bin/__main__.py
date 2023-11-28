#!/usr/bin/env python
# -*- coding: utf-8 -*-
import logging
import os
import sys
from logging import config as logging_config

import yaml

from ydb.tools.cfg.configurator_setup import get_parser, parse_optional_arguments
from ydb.tools.cfg.dynamic import DynamicConfigGenerator
from ydb.tools.cfg.static import StaticConfigGenerator
from ydb.tools.cfg.utils import write_to_file
from ydb.tools.cfg.walle import NopHostsInformationProvider, WalleHostsInformationProvider

logging_config.dictConfig(
    {
        "version": 1,
        "formatters": {
            "base": {
                "format": "%(asctime)s - %(processName)s - %(name)s - %(levelname)s - %(message)s",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": "INFO",
                "formatter": "base",
                "stream": sys.stdout,
            },
        },
        "root": {"formatter": "base", "level": "INFO", "handlers": ("console",)},
    }
)


logger = logging.getLogger()


def cfg_generate(args):
    kwargs = parse_optional_arguments(args)

    if args.dynamic:
        cfg_cls = DynamicConfigGenerator
    else:
        cfg_cls = StaticConfigGenerator

    with open(args.cluster_description, "r") as yaml_template:
        cluster_template = yaml.safe_load(yaml_template)

    hosts_provider = NopHostsInformationProvider()
    if args.hosts_provider_url:
        hosts_provider = WalleHostsInformationProvider(args.hosts_provider_url)

    generator = cfg_cls(cluster_template, args.binary_path, args.output_dir, walle_provider=hosts_provider, **kwargs)

    all_configs = generator.get_all_configs()
    for cfg_name, cfg_value in all_configs.items():
        write_to_file(os.path.join(args.output_dir, cfg_name), cfg_value)


def main():
    parser = get_parser(cfg_generate, [{"name": "--hosts-provider-url", "help": "URL from which information about hosts can be obtained."}])
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    exit(main())
