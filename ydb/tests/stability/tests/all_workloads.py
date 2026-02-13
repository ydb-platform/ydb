"""Workload definitions for stability tests.

This module provides a dictionary of workload configurations that is initialized
only once when first imported, even when imported from multiple places.
"""
from copy import deepcopy
import logging
import yatest.common

_all_stress_utils = None


def _init_stress_utils():
    """Initialize the workloads dictionary (called once on first import)."""
    global _all_stress_utils
    if _all_stress_utils is not None:
        return

    _all_stress_utils = {
        'Cdc': {
            'args': ["--endpoint", "grpc://{node_host}:2135",],
            'local_path': 'ydb/tests/stress/cdc/cdc'
        },
        'Ctas': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--path", "workload_ctas_{node_host}_iter_{iteration_num}_{uuid}"],
            'local_path': 'ydb/tests/stress/ctas/ctas'
        },
        'Kafka': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--bootstrap", "http://{node_host}:11223",
                     "--source-path", "workload_source_kafka_{node_host}_iter_{iteration_num}_{uuid}",
                     "--target-path", "workload_target_kafka_{node_host}_iter_{iteration_num}_{uuid}",
                     "--consumer", "workload-consumer-{iteration_num}-{uuid}",
                     "--num-workers", "2"],
            'local_path': 'ydb/tests/stress/kafka/kafka_streams_test'
        },
        'NodeBroker': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--mon-endpoint", "http://{node_host}:8765",
                     ],
            'local_path': 'ydb/tests/stress/node_broker/node_broker'
        },
        'Olap': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--path", "olap_workload_{node_host}_iter_{iteration_num}_{uuid}"],
            'local_path': 'ydb/tests/stress/olap_workload/olap_workload'
        },
        'Oltp': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--path", "oltp_workload_{node_host}_iter_{iteration_num}_{uuid}"],
            'local_path': 'ydb/tests/stress/oltp_workload/oltp_workload'
        },
        # TODO does not support custom cluster configs
        # 'workload_reconfig_state_storage': {
        #     'args': ,
        #     'local_path': 'ydb/tests/stress/reconfig_state_storage_workload/reconfig_state_storage_workload'
        # },
        'ShowCreateView': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--path-prefix", "workload_show_create_{node_host}_iter_{iteration_num}_{uuid}"],
            'local_path': 'ydb/tests/stress/show_create/view/show_create_view'
        },
        'ShowCreateTable': {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--path-prefix", "workload_show_create_{node_host}_iter_{iteration_num}_{uuid}"],
            'local_path': 'ydb/tests/stress/show_create/table/show_create_table'
        },
        'Statistics': {
            'args': ["--host", "{node_host}",
                     "--port", "2135",
                     "--prefix", "statistics_workload_{node_host}_iter_{iteration_num}_{uuid}"],
            'local_path': 'ydb/tests/stress/statistics_workload/statistics_workload'
        },
        'TopicKafka': {
            'args': [
                "--endpoint", "grpc://{node_host}:2135",
                "--topic_prefix", "workload_source_topic_kafka_{node_host}_iter_{iteration_num}_{uuid}",
                "--duration", "120",
                "--consumers", "2",
                "--consumer-threads", "2",
                "--restart-interval", "15s",
                "--partitions", "4",
                "--write-workload", "0.01", "9000000", "2", "big_record", "1",
                "--write-workload", "8000", "45", "1000", "small_record", "10",
                "--write-workload", "800", "4096", "1", "medium_record", "10",
            ],
            'local_path': 'ydb/tests/stress/topic_kafka/workload_topic_kafka'
        },
        'Topic': {
            'args': [
                "--endpoint", "grpc://{node_host}:2135",
                "--topic_prefix", "workload_topic_{node_host}_iter_{iteration_num}_{uuid}",
            ],
            'local_path': 'ydb/tests/stress/topic/workload_topic'
        },
        'Viewer': {
            'args': [
                "--mon_endpoint", "http://{node_host}:8765",
            ],
            'local_path': 'ydb/tests/stress/viewer/viewer'
        },
        'TestShard': {
            'args': [
                "--endpoint", "grpc://{node_host}:2135",
                "--owner-idx", "{global_run_id}"
            ],
            'local_path': 'ydb/tests/stress/testshard_workload/workload_testshard'
        },
        'IncrementalBackup': {
            'args': [
                "--endpoint", "grpc://{node_host}:2135",
                "--backup-interval", "20"
            ],
            'local_path': 'ydb/tests/stress/backup/backup_stress'
        },
        'Streaming': {
            'args': [
                "--endpoint", "{node_host}:2135",
                "--partitions-count", "10",
                "--prefix", "streaming_stress/run_{global_run_id}"
            ],
            'local_path': 'ydb/tests/stress/streaming/streaming'
        },
    }

    for table_type in ['row', 'column']:
        _all_stress_utils[f'Kv_{table_type}'] = {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--store_type", table_type, "--kv_prefix", f"workload_kv_{table_type}_{{node_host}}_iter_{{iteration_num}}_{{uuid}}"],
            'local_path': 'ydb/tests/stress/kv/workload_kv'
        }
        _all_stress_utils[f'Log_{table_type}'] = {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--store_type", table_type,
                     "--log_prefix", f"log_{table_type}_{{node_host}}_iter_{{iteration_num}}_{{uuid}}"],
            'local_path': 'ydb/tests/stress/log/workload_log'
        }
        _all_stress_utils[f'Mixed_{table_type}'] = {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--store_type", table_type,
                     "--mixed_prefix", f"mixed_{table_type}_{{node_host}}_iter_{{iteration_num}}_{{uuid}}"],
            'local_path': 'ydb/tests/stress/mixedpy/workload_mixed'
        }
        _all_stress_utils[f'SimpleQueue_{table_type}'] = {
            'args': ["--endpoint", "grpc://{node_host}:2135",
                     "--mode", table_type],
            'local_path': 'ydb/tests/stress/simple_queue/simple_queue'
        }
        for topic_type in ['local', 'remote']:
            _all_stress_utils[f'Transfer_{table_type}_topic_{topic_type}'] = {
                'args': [
                    "--endpoint", "grpc://{node_host}:2135",
                    "--mode", table_type,
                    "--topic", topic_type,
                ],
                'local_path': 'ydb/tests/stress/transfer/transfer'
            }

    for config_preset in ['common_channel_read', 'inline_channel_read', 'write_read_delete']:
        _all_stress_utils[f'KVVolume_{config_preset}'] = {
            'args': ["--endpoint", "grpc://{node_host}:2135", '--in-flight', '3', '--version', 'v1', '--config-name', config_preset],
            'local_path': 'ydb/tests/stress/kv_volume/workload_keyvalue_volume'
        }

    filtered_stress_utils_arg: str = yatest.common.get_param('stress-utils-to-run', None)

    if filtered_stress_utils_arg:
        filtered_stress_utils = filtered_stress_utils_arg.split(',')
        # ! prefix is used for exclusion filtering
        ignored_stress_utils = [util_name[1:].lower() for util_name in filtered_stress_utils if util_name.startswith('!')]
        filtered_stress_utils = [util_name.lower() for util_name in filtered_stress_utils if not util_name.startswith('!')]

        logging.info(f'Filtered {filtered_stress_utils} stress utils')
        logging.info(f'Ignoring {ignored_stress_utils} stress utils')

        if len(filtered_stress_utils) == 0:
            _all_stress_utils = {k: v for k, v in _all_stress_utils.items() if all(ignored_util not in k.lower() for ignored_util in ignored_stress_utils)}
        else:
            _all_stress_utils = {k: v for k, v in _all_stress_utils.items() if
                                 any(filtered_util in k.lower() for filtered_util in filtered_stress_utils) and
                                 all(ignored_util not in k.lower() for ignored_util in ignored_stress_utils)}

    logging.info(f'Using {get_all_stress_names()} stress utils')


def get_all_stress_names():
    return list(_all_stress_utils.keys())


# Initialize on import
_init_stress_utils()


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
