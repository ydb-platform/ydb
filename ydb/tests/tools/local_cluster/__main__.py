#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Utility to start local YDB cluster (ydbd) on default ports.
Uses harness from ydb/tests/library/harness to start cluster with MIRROR_3DC configuration.
First node starts on default ports (grpc=2135, mon=8765, ic=19001).
"""
import argparse
import logging
import os
import signal
import sys
import time
import urllib.request
import urllib.error

from ydb.tests.library.harness import kikimr_config
from ydb.tests.library.harness import kikimr_runner
from ydb.tests.library.harness.kikimr_port_allocator import (
    DefaultFirstNodePortAllocator,
)
from ydb.tests.library.common.types import Erasure

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def download_binary_from_s3(version, working_dir):
    """
    Downloads ydbd binary from S3 storage if not already downloaded.
    Each version is stored in a separate directory: binary/<version>/ydbd

    Args:
        version: Git ref/version (e.g., '25.3.1.21')
        working_dir: Base working directory

    Returns:
        Path to downloaded binary
    """
    binary_dir = os.path.join(working_dir, "binary", version)
    binary_path = os.path.join(binary_dir, "ydbd")

    if os.path.exists(binary_path) and os.access(binary_path, os.X_OK):
        logger.info("Binary already exists for version %s: %s", version, binary_path)
        return binary_path

    url = f"https://storage.yandexcloud.net/ydb-builds/{version}/release/ydbd"

    logger.info("Downloading ydbd binary from S3...")
    logger.info("Version: %s", version)
    logger.info("URL: %s", url)
    logger.info("Destination: %s", binary_path)

    try:
        os.makedirs(binary_dir, exist_ok=True)
        urllib.request.urlretrieve(url, binary_path)
        os.chmod(binary_path, 0o755)

        logger.info("Binary downloaded successfully")
        return binary_path
    except Exception as e:
        logger.error("Unexpected error during download: %s", e)
        raise RuntimeError(f"Failed to download binary: {e}") from e


class LocalCluster:
    def __init__(self, working_dir, binary_path=None, enable_nbs=False, port_offset=0):
        self.cluster = None
        self.working_dir = working_dir
        self.binary_path = binary_path
        self.enable_nbs = enable_nbs
        self.port_offset = port_offset

    def start(self):
        """Starts local YDB cluster with MIRROR_3DC configuration on default ports."""
        logger.info("Initializing local YDB cluster with MIRROR_3DC configuration...")

        if not self.binary_path:
            raise RuntimeError("binary_path must be set before calling start()")

        logger.info("Using ydbd binary: %s", self.binary_path)

        # Use port allocator with offset to allow multiple instances
        port_allocator = DefaultFirstNodePortAllocator(base_offset=self.port_offset)

        nbs_database = "/Root/NBS"
        configurator = kikimr_config.KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            port_allocator=port_allocator,
            output_path=self.working_dir,
            binary_paths=[self.binary_path],
            enable_nbs=self.enable_nbs,
            nbs_database=nbs_database,
        )

        self.cluster = kikimr_runner.KiKiMR(
            configurator=configurator,
            cluster_name='local_cluster'
        )

        logger.info("Starting cluster...")
        try:
            self.cluster.start()
        except Exception as e:
            error_msg = "Failed to start cluster.\n\n"
            error_msg += "\nOriginal error:\n"
            error_msg += str(e)
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        if self.enable_nbs:
            self.start_nbs(nbs_database)

        logger.info("Cluster started successfully!")
        logger.info("Total nodes: %d", len(self.cluster.nodes))
        for node_id, node in sorted(self.cluster.nodes.items()):
            logger.info(
                "Node %d: GRPC=%s, MON=%s, IC=%s, Endpoint=%s",
                node_id, node.grpc_port, node.mon_port, node.ic_port, node.endpoint
            )

        if len(self.cluster.slots) > 0:
            logger.info("Total dynamic nodes: %d", len(self.cluster.slots))
            for node_id, node in sorted(self.cluster.slots.items()):
                logger.info(
                    "Dynamic Node %d: GRPC=%s, MON=%s, IC=%s, Endpoint=%s",
                    node_id, node.grpc_port, node.mon_port, node.ic_port, node.endpoint
                )

        first_node = self.cluster.nodes[1]
        logger.info("First node working directory: %s", first_node.cwd)

        return self.cluster

    def stop(self):
        """Stops the cluster."""
        if self.cluster:
            logger.info("Stopping cluster...")
            self.cluster.stop()
            logger.info("Cluster stopped")
            self.cluster = None

    def wait(self):
        """Waits for cluster termination (until signal is received)."""
        try:
            while True:
                time.sleep(1)
                if self.cluster and self.cluster.nodes:
                    for node_id, node in self.cluster.nodes.items():
                        if node.daemon.process.poll() is not None:
                            logger.error("ydbd process for node %d terminated unexpectedly", node_id)
                            logger.error("Stopping cluster due to unexpected node termination")
                            self.stop()
                            return
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")

    def start_nbs(self, nbs_database):
        logger.info("Creating NBS database: %s", nbs_database)
        self.cluster.create_database(
            nbs_database,
            storage_pool_units_count={
                'hdd': 9
            }
        )
        # logger.info("Database created, waiting for it to be fully ready...")
        # # Give the database a moment to propagate through the cluster
        # time.sleep(3)

        # # Verify database exists before starting slots
        # try:
        #     status = self.cluster.get_database_status(nbs_database)
        #     logger.info("Database status: %s", status)
        # except Exception as e:
        #     logger.warning("Could not get database status: %s", e)

        logger.info("Registering and starting NBS dynamic slots...")
        slots = self.cluster.register_and_start_slots(nbs_database, count=1)

        logger.info("Waiting for NBS tenant to be up...")
        try:
            self.cluster.wait_tenant_up(nbs_database)
            logger.info("NBS tenant is ready")
        except Exception as e:
            logger.error("Failed to start NBS tenant: %s", e)
            raise

        return slots


def setup_signal_handlers(cluster):
    """Sets up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        logger.info("Received signal %s, stopping cluster...", signum)
        cluster.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    parser = argparse.ArgumentParser(
        description='Utility to start local YDB cluster (ydbd) with MIRROR_3DC configuration on default ports'
    )
    parser.add_argument(
        '--working-dir',
        type=str,
        default='.ydbd_working_dir',
        help='Working directory for cluster data storage (default: .ydbd_working_dir in current directory)'
    )
    parser.add_argument(
        '--binary-path',
        type=str,
        default=None,
        help='Path to ydbd binary (overrides --version if specified)'
    )
    parser.add_argument(
        '--version',
        type=str,
        default="main",
        help='Git ref/version to download from S3 (e.g., 25.3.1.21). Binary will be downloaded to working-dir. Ignored if --binary-path is specified'
    )
    parser.add_argument(
        '--enable-nbs',
        action='store_true',
        help='Enable NBS (Network Block Storage) configuration and dynamic nodes'
    )
    parser.add_argument(
        '--port-offset',
        type=int,
        default=0,
        help='Port offset to avoid conflicts with other instances (default: 0). '
             'With offset N: GRPC=2135+N, MON=8765+N, IC=19001+N. '
             'Example: --port-offset=1000 for GRPC=3135, MON=9765, IC=20001'
    )

    args = parser.parse_args()

    working_dir = os.path.abspath(args.working_dir)

    if args.binary_path is not None:
        binary_path = args.binary_path
        if not os.path.exists(binary_path):
            logger.error("Binary path does not exist: %s", binary_path)
            sys.exit(1)
        if not os.access(binary_path, os.X_OK):
            logger.error("Binary path is not executable: %s", binary_path)
            sys.exit(1)
    else:
        binary_path = download_binary_from_s3(args.version, working_dir)

    cluster_manager = LocalCluster(
        working_dir=working_dir,
        binary_path=binary_path,
        enable_nbs=args.enable_nbs,
        port_offset=args.port_offset
    )

    try:
        cluster_manager.start()
        setup_signal_handlers(cluster_manager)

        logger.info("Cluster is running. Press Ctrl+C to stop.")
        cluster_manager.wait()

    except Exception as e:
        logger.exception("Error during cluster operation: %s", e)

        cluster_manager.stop()
        sys.exit(1)


if __name__ == '__main__':
    main()
