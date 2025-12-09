"""Workload definitions for stability tests.

This module provides a dictionary of workload configurations that is initialized
only once when first imported, even when imported from multiple places.
"""
from copy import deepcopy


_all_stress_utils = None


def _init_stress_utils():
    """Initialize the workloads dictionary (called once on first import)."""
    global _all_stress_utils
    if _all_stress_utils is not None:
        return

    _all_stress_utils = {
        'IncrementalBackup': {
            'args': [
                "--endpoint", "grpc://{node_host}:2135",
                "--backup-interval", "20"
            ],
            'local_path': 'ydb/tests/stress/backup/backup_stress'
        },
    }


# Initialize on import
_init_stress_utils()


def get_all_stress_names():
    return list(_all_stress_utils.keys())


def get_stress_util(name, common_args):
    """Get all available workload configurations with additional common arguments.

    Args:
        common_args: List of additional arguments that will be appended to each
                    workload's argument list. These typically include cluster-specific
                    parameters that are common across all workloads (["--database", f"/{YdbCluster.ydb_database}"]).

    Returns:
        Dictionary containing all workload configurations, where each entry has:
        - 'args': List of arguments for the workload (including common_args)
        - 'local_path': Path to the workload executable
    """
    workload_copy = deepcopy(_all_stress_utils[name])
    workload_copy['args'] += common_args
    return workload_copy


def get_all_stress_utils(common_args):
    """Get all available workload configurations with additional common arguments.

    Args:
        common_args: List of additional arguments that will be appended to each
                    workload's argument list. These typically include cluster-specific
                    parameters that are common across all workloads (["--database", f"/{YdbCluster.ydb_database}"]).

    Returns:
        Dictionary containing all workload configurations, where each entry has:
        - 'args': List of arguments for the workload (including common_args)
        - 'local_path': Path to the workload executable
    """
    workload_copy = deepcopy(_all_stress_utils)
    for wl, arg in workload_copy.items():
        arg['args'] += common_args
    return workload_copy
