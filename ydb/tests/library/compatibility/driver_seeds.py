# -*- coding: utf-8 -*-
import logging

from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


def cluster_units(cluster):
    units = list(cluster.nodes.values())
    if cluster.slots:
        units.extend(cluster.slots.values())
    return units


def driver_seed_endpoints(cluster, skip_unit=None):
    alive = []
    fallback = []
    for unit in cluster_units(cluster):
        if skip_unit is not None and unit is skip_unit:
            continue
        fallback.append(unit.grpc_endpoint)
        if unit.is_alive():
            alive.append(unit.grpc_endpoint)
        else:
            logger.warning("Skipping dead cluster unit %s as driver seed", unit)
    return alive or fallback


def create_ydb_driver(database_path, cluster, skip_unit=None, timeout=60):
    seeds = driver_seed_endpoints(cluster, skip_unit=skip_unit)
    if not seeds:
        raise RuntimeError("No gRPC endpoints available to create YDB driver")

    discovery_timeout = max(1, min(5, int(timeout)))
    driver = ydb.Driver(
        ydb.DriverConfig(
            database=database_path,
            endpoint=seeds[0],
            endpoints=seeds[1:],
            discovery_request_timeout=discovery_timeout,
        )
    )
    try:
        driver.wait(timeout=timeout)
        return driver
    except Exception:
        driver.stop()
        raise
