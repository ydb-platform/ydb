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
from ydb.tools.cfg.utils import write_to_file, backport
from ydb.tools.cfg.walle import NopHostsInformationProvider, WalleHostsInformationProvider
from ydb.tools.cfg.k8s_api import K8sApiHostsInformationProvider

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

    host_info_provider = NopHostsInformationProvider()

    k8s_enabled = cluster_template.get("k8s_settings", {}).get("use", False)
    walle_enabled = cluster_template.get("use_walle", False)

    if args.hosts_provider_url:
        if not walle_enabled:
            raise RuntimeError("you specified --hosts-provider-url, but `use_walle` is false in template.\nSpecify `use_walle: True` to continue")
        host_info_provider = WalleHostsInformationProvider(args.hosts_provider_url)
    elif k8s_enabled:
        host_info_provider = K8sApiHostsInformationProvider(args.kubeconfig)

    if walle_enabled and not isinstance(host_info_provider, WalleHostsInformationProvider):
        raise RuntimeError("you specified 'use_walle: True', but didn't specify --hosts-provider-url to initialize walle")

    if walle_enabled and k8s_enabled:
        raise RuntimeError("you specified 'use_walle: True' and 'k8s_settings.use: True', please select a single host info provider")

    if not walle_enabled and not k8s_enabled:
        logger.warning("you didn't specify any host info provider (neither walle nor k8s). Make sure you know what you are doing")

    generator = cfg_cls(cluster_template, args.binary_path, args.output_dir, host_info_provider=host_info_provider, **kwargs)

    all_configs = generator.get_all_configs()
    for cfg_name, cfg_value in all_configs.items():
        write_to_file(os.path.join(args.output_dir, cfg_name), cfg_value)

    if args.backport_to_template:
        backport(args.cluster_description, all_configs["config.yaml"], ["blob_storage_config"])


def main():
    parser = get_parser(cfg_generate)
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    exit(main())
