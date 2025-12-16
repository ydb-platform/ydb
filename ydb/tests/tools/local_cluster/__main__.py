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


class LocalCluster:
    def __init__(self, working_dir=None, binary_path=None):
        self.cluster = None
        # Default working directory if not specified
        if working_dir is None:
            working_dir = os.path.abspath(".ydbd_working_dir")
        self.working_dir = working_dir
        self.binary_path = binary_path

    def start(self):
        """Starts local YDB cluster with MIRROR_3DC configuration on default ports."""
        logger.info("Initializing local YDB cluster with MIRROR_3DC configuration...")

        if not self.binary_path:
            raise RuntimeError("binary_path must be set before calling start()")

        logger.info("Using ydbd binary: %s", self.binary_path)

        port_allocator = DefaultFirstNodePortAllocator()

        configurator = kikimr_config.KikimrConfigGenerator(
            erasure=Erasure.MIRROR_3_DC,
            port_allocator=port_allocator,
            output_path=self.working_dir,
            binary_paths=[self.binary_path],
        )

        self.cluster = kikimr_runner.KiKiMR(
            configurator=configurator,
            cluster_name='local_cluster'
        )

        logger.info("Starting cluster...")
        try:
            self.cluster.start()
        except Exception as e:
            # On any error during startup, add port information
            error_msg = "Failed to start cluster.\n\n"
            error_msg += "\nOriginal error:\n"
            error_msg += str(e)
            logger.error(error_msg)
            raise RuntimeError(error_msg) from e

        logger.info("Cluster started successfully!")
        logger.info("Total nodes: %d", len(self.cluster.nodes))
        for node_id, node in sorted(self.cluster.nodes.items()):
            logger.info(
                "Node %d: GRPC=%s, MON=%s, IC=%s, Endpoint=%s",
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
                # Check that processes are still alive
                if self.cluster and self.cluster.nodes:
                    for node_id, node in self.cluster.nodes.items():
                        if node.daemon.process.poll() is not None:
                            logger.error("ydbd process for node %d terminated unexpectedly", node_id)
                            logger.error("Stopping cluster due to unexpected node termination")
                            self.stop()
                            return
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")


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
        default=None,
        help='Working directory for cluster data storage (default: .ydbd_working_dir in current directory)'
    )
    parser.add_argument(
        '--binary-path',
        type=str,
        required=True,
        help='Path to ydbd binary'
    )

    args = parser.parse_args()

    # Validate binary path
    if not os.path.exists(args.binary_path):
        logger.error("Binary path does not exist: %s", args.binary_path)
        sys.exit(1)
    if not os.access(args.binary_path, os.X_OK):
        logger.error("Binary path is not executable: %s", args.binary_path)
        sys.exit(1)

    cluster_manager = LocalCluster(working_dir=args.working_dir, binary_path=args.binary_path)

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
