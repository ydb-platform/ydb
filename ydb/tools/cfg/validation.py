# -*- coding: utf-8 -*-
import collections
import copy

import jsonschema

from ydb.tools.cfg import utils
from ydb.tools.cfg.types import Erasure, FailDomainType, LogLevels, NodeType, PDiskCategory

KQP_SCHEMA = {
    "type": "object",
    "properties": {
        "enable": {"type": "boolean"},
    },
}

# Custom profiles to create.
# These profiles will be on top of channels.txt file so
# user can influence on id of these profiles
PROFILES = {
    "type": "array",
    "items": {
        "type": "object",
        "properties": {
            "channels": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "pdisk_type": {
                            "type": "string",
                            "enum": PDiskCategory.all_pdisk_category_names(),
                        },
                        "erasure": {
                            "type": "string",
                            "enum": Erasure.all_erasure_type_names(),
                        },
                        "vdisk_kind": {
                            "type": "string",
                        },
                        "storage_pool_kind": {
                            "type": "string",
                        },
                    },
                    "additionalProperties": False,
                },
                "minItems": 3,
            }
        },
    },
}


FEATURES_SCHEMA = {
    "type": "object",
    "properties": {},
}

EXECUTOR_SCHEMA = {
    "type": "object",
    "properties": {
        "threads": {
            "type": "integer",
            "min": 1,
        },
        "spin_threshold": {
            "type": "integer",
            "min": 1,
        },
        "TimePerMailboxMicroSecs": {
            "type": "integer",
            "min": 1,
        },
        "time_per_mailbox_micro_secs": {
            "type": "integer",
            "min": 1,
        },
        "max_threads": {
            "type": "integer",
            "min": 1,
        },
        "priority": {
            "type": "integer",
            "min": 1,
        },
        "max_avg_ping_deviation": {
            "type": "integer",
            "min": 1,
        },
    },
}

SYS_SCHEMA = {
    "type": "object",
    "properties": {
        "preset_name": {
            "type": "string",
            "enum": utils.get_resources_list("resources/sys/"),
        },
        "executors": {
            "type": "object",
            "properties": {
                "system": copy.deepcopy(EXECUTOR_SCHEMA),
                "batch": copy.deepcopy(EXECUTOR_SCHEMA),
                "user": copy.deepcopy(EXECUTOR_SCHEMA),
                "io": copy.deepcopy(EXECUTOR_SCHEMA),
                "ic": copy.deepcopy(EXECUTOR_SCHEMA),
            },
            "additionalProperties": False,
        },
        "scheduler": {
            "type": "object",
            "properties": {
                "resolution": {"type": "integer"},
                "spin_threshold": {"type": "integer"},
                "progress_threshold": {"type": "integer"},
            },
        },
        "use_auto_config": {"type": "boolean"},
        "cpu_count": {"type": "integer"},
        "node_type": {
            "type": "string",
            "enum": NodeType.all_node_type_names(),
        },
        "force_io_pool_threads": {"type": "integer"},
    },
    "additionalProperties": False,
}

SELECTORS_CONFIGS = dict(
    type="object",
    properties=dict(
        request_type=dict(type="string"),
    ),
    required=[],
    additionalProperties=False,
)

TRACING_SCHEMA = dict(
    type="object",
    properties=dict(
        backend=dict(
            type="object",
            properties=dict(
                auth_config=dict(
                    type="object",
                    properties=dict(
                        tvm=dict(
                            type="object",
                            properties=dict(
                                url=dict(type="string"),
                                self_tvm_id=dict(type="integer"),
                                tracing_tvm_id=dict(type="integer"),
                                disc_cache_dir=dict(type="string"),
                                plain_text_secret=dict(type="string"),
                                secret_file=dict(type="string"),
                                secret_environment_variable=dict(type="string"),
                            ),
                            required=["self_tvm_id", "tracing_tvm_id"],
                        )
                    ),
                    required=["tvm"],
                ),
                opentelemetry=dict(
                    type="object",
                    properties=dict(
                        collector_url=dict(type="string"),
                        service_name=dict(type="string"),
                    )
                ),
            ),
            required=["opentelemetry"],
            additionalProperties=False,
        ),
        uploader=dict(
            type="object",
            properties=dict(
                max_exported_spans_per_second=dict(type="integer", minimum=1),
                max_spans_in_batch=dict(type="integer", minimum=1),
                max_bytes_in_batch=dict(type="integer"),
                max_batch_accumulation_milliseconds=dict(type="integer"),
                span_export_timeout_seconds=dict(type="integer", minimum=1),
                max_export_requests_inflight=dict(type="integer", minimum=1),
            ),
            additionalProperties=False,
        ),
        sampling=dict(
            type="array",
            items=dict(
                type="object",
                properties=dict(
                    scope=SELECTORS_CONFIGS,
                    fraction=dict(type="number", minimum=0, maximum=1),
                    level=dict(type="integer", minimum=0, maximum=15),
                    max_traces_per_minute=dict(type="integer", minimum=0),
                    max_traces_burst=dict(type="integer", minimum=0),
                ),
                required=["fraction", "level", "max_traces_per_minute"],
            ),
        ),
        external_throttling=dict(
            type="array",
            items=dict(
                type="object",
                properties=dict(
                    scope=SELECTORS_CONFIGS,
                    max_traces_per_minute=dict(type="integer", minimum=0),
                    max_traces_burst=dict(type="integer", minimum=0),
                ),
                required=["max_traces_per_minute"],
            ),
        ),
    ),
    required=["backend"],
    additionalProperties=False,
)

FAILURE_INJECTION_CONFIG_SCHEMA = {
    "type": "object",
    "properties": {"approximate_termination_interval": dict(type="integer")},
    "additionalProperties": False,
}

DRIVE_SCHEMA = {
    "type": "object",
    "properties": {
        "type": dict(type="string", enum=PDiskCategory.all_pdisk_category_names()),
        "path": dict(type="string", minLength=1),
        "shared_with_os": dict(type="boolean"),
        "expected_slot_count": dict(type="integer"),
        "kind": dict(type="integer"),
    },
    "required": ["type", "path"],
    "additionalProperties": False,
}

HOST_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "minLength": 1},
        "drives": {"type": "array", "items": copy.deepcopy(DRIVE_SCHEMA)},
        "host_config_id": {"type": "integer", "minLength": 1},
        "ic_port": {
            "type": "integer",
        },
        "node_id": {"type": "integer", "minLength": 1},
    },
    "required": [
        "name",
    ],
}

LOG_SCHEMA = {
    "type": "object",
    "properties": {
        "default": {"type": "integer", "min": int(min(LogLevels)), "max": int(max(LogLevels))},
        "entries": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "minLength": 1},
                    "level": {
                        "type": "integer",
                        "min": int(min(LogLevels)),
                        "max": int(max(LogLevels)),
                    },
                },
            },
        },
    },
}

STORAGE_POOL = {
    "type": "object",
    "properties": {
        "erasure": {
            "type": "string",
            "minLength": 1,
            "enum": Erasure.all_erasure_type_names(),
        },
        "generation": {"type": "integer", "min": 0},
        "encryption_mode": {"type": "integer", "min": 0, "max": 1},
        "num_groups": {
            "type": "integer",
            "min": 1,
        },
        "storage_pool_id": {"type": "integer", "min": 1},
        "fail_domain_type": {
            "type": "string",
            "minLength": 1,
            "enum": FailDomainType.all_fail_domain_type_names(),
        },
        "kind": {
            "type": "string",
            "minLength": 1,
        },
        "name": {
            "type": "string",
            "minLength": 1,
        },
        "filter_properties": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": PDiskCategory.all_pdisk_category_names(),
                },
                "SharedWithOs": {
                    "type": "boolean",
                },
                "kind": dict(type="integer"),
            },
            "required": ["type"],
            "additionalProperties": False,
        },
        "vdisk_kind": {
            "type": "string",
            "minLength": 1,
        },
    },
    "required": [
        "num_groups",
        "erasure",
        "filter_properties",
    ],
    "additionalProperties": False,
}

STORAGE_POOL_KIND = {
    "type": "object",
    "properties": {
        "kind": {
            "type": "string",
            "minLength": 1,
        },
        "erasure": {
            "type": "string",
            "minLength": 1,
            "enum": Erasure.all_erasure_type_names(),
        },
        "encryption_mode": {"type": "integer", "min": 0, "max": 1},
        "filter_properties": {
            "type": "object",
            "properties": {
                "type": {
                    "type": "string",
                    "enum": PDiskCategory.all_pdisk_category_names(),
                },
                "SharedWithOs": {
                    "type": "boolean",
                },
                "kind": dict(type="integer"),
            },
            "required": ["type"],
            "additionalProperties": False,
        },
        "fail_domain_type": {
            "type": "string",
            "minLength": 1,
            "enum": FailDomainType.all_fail_domain_type_names(),
        },
        "vdisk_kind": {
            "type": "string",
            "minLength": 1,
        },
    },
    "required": ["kind", "erasure", "filter_properties"],
}

STORAGE_POOL_INSTANCE = {
    "type": "object",
    "properties": {
        "kind": {
            "type": "string",
            "minLength": 1,
        },
        "num_groups": {
            "type": "integer",
            "min": 1,
        },
        "generation": {
            "type": "integer",
            "min": 0,
        },
    },
    "required": [
        "kind",
        "num_groups",
    ],
    "additionalProperties": False,
}


SHARED_CACHE_SCHEMA = {
    "type": "object",
    "properties": {"memory_limit": {"type": "integer"}},
    "additionalProperties": False,
}

TENANT_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string",
            "minLength": 1,
        },
        "storage_units": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "count": {
                        "type": "integer",
                    },
                    "kind": {"type": "string", "minLength": 1},
                },
            },
        },
        "compute_units": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "count": {
                        "type": "integer",
                        "min": 1,
                    },
                    "kind": {
                        "type": "string",
                        "minLength": 1,
                    },
                    "zone": {"type": "string", "minLength": 1},
                    "required": ["count", "kind", "zone"],
                    "additionalProperties": False,
                },
            },
        },
        "overridden_configs": {
            "type": "object",
        },
    },
}

DOMAIN_SCHEMA = {
    "type": "object",
    "properties": {
        "domain_name": {"type": "string", "minLength": 1},
        "plan_resolution": {
            "type": "integer",
            "min": 1,
        },
        "domain_id": {"type": "integer", "min": 1},
        "mediators": {
            "type": "integer",
            "min": 1,
        },
        "coordinators": {"type": "integer", "min": 1},
        "allocators": {"type": "integer", "min": 1},
        "storage_pool_kinds": {"type": "array", "items": copy.deepcopy(STORAGE_POOL_KIND)},
        "storage_pools": {"type": "array", "items": copy.deepcopy(STORAGE_POOL_INSTANCE)},
        "databases": {
            "type": "array",
            "items": copy.deepcopy(TENANT_SCHEMA),
        },
        "config_cookie": {
            "type": "string",
        },
        "dynamic_slots": {"type": "integer", "min": 1},
        "bind_slots_to_numa_nodes": {"type": "boolean"},
        "console_initializers": {
            "type": "array",
            "items": {
                "type": "string",
                "enum": utils.get_resources_list("resources/console_initializers/"),
            },
        },
    },
    "required": ["domain_name"],
    "additionalProperties": False,
}

NBS_SCHEMA = {
    "type": "object",
    "properties": {
        "diagnostics": {"type": "object"},
        "enable": {"type": "boolean"},
        "new_config_generator_enabled": {"type": "boolean"},
        "sys": copy.deepcopy(SYS_SCHEMA),
        "log": copy.deepcopy(LOG_SCHEMA),
        "domain": {"type": "string"},
        "subdomain": {"type": "string"},
        "storage": {"type": "object", "properties": {}},
        "disk_registry_proxy": {
            "type": "object",
        },
        "server": {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                },
                "port": {
                    "type": "integer",
                },
                "root_certs_file": {
                    "type": "string",
                },
                "certs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cert_file": {
                                "type": "string",
                            },
                            "cert_private_key_file": {
                                "type": "string",
                            },
                        },
                    },
                },
            },
        },
        "discovery": {"type": "object"},
        "ydbstats": {"type": "object"},
        "auth": {"type": "object"},
        "names": {"type": "object"},
        "client": {
            "type": "object",
            "properties": {
                "auth_config": {"type": "object"},
                "client_config": {"type": "object"},
                "log_config": {"type": "object"},
                "monitoring_config": {"type": "object"},
                "throttling_enabled": {"type": "boolean"},
                "throttling_enabled_s_s_d": {"type": "boolean"},
                "throttling_disabled_s_s_d_nonrepl": {"type": "boolean"},
            },
            "additionalProperties": False,
        },
        "http_proxy": {
            "type": "object",
            "properties": {
                "port": {
                    "type": "integer",
                },
                "secure_port": {
                    "type": "integer",
                },
                "certs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "cert_file": {
                                "type": "string",
                            },
                            "cert_private_key_file": {
                                "type": "string",
                            },
                        },
                    },
                },
                "nbs_server_host": {
                    "type": "string",
                },
                "nbs_server_port": {
                    "type": "integer",
                },
                "nbs_server_cert_file": {
                    "type": "string",
                },
                "root_certs_file": {
                    "type": "string",
                },
                "nbs_server_insecure": {
                    "type": "boolean",
                },
            },
            "additionalProperties": False,
        },
        "breakpad": {
            "type": "object",
            "properties": {
                "enable": {
                    "type": "boolean",
                },
            },
            "additionalProperties": False,
        },
        "breakpad_sender": {
            "type": "object",
            "properties": {
                "aggregator_url": {
                    "type": "string",
                },
                "notify_email": {
                    "type": "string",
                },
            },
            "additionalProperties": False,
        },
        "logbroker": {
            "type": "object",
            "properties": {
                "address": {"type": "string"},
                "port": {"type": "integer"},
                "database": {"type": "string"},
                "use_logbroker_c_d_s": {"type": "boolean"},
                "ca_cert_filename": {"type": "string"},
                "topic": {"type": "string"},
                "source_id": {"type": "string"},
                "metadata_server_address": {"type": "string"},
            },
            "additionalProperties": False,
        },
        "notify": {"type": "object", "properties": {"endpoint": {"type": "string"}}},
        "iam": {
            "type": "object",
        },
        "kms": {
            "type": "object",
        },
        "compute": {
            "type": "object",
        },
        "features": {
            "type": "object",
            "properties": {
                "features": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string",
                            },
                            "Whitelist": {
                                "type": "object",
                                "properties": {
                                    "cloud_ids": {
                                        "type": "array",
                                        "items": {
                                            "type": "string",
                                        },
                                    }
                                },
                            },
                            "Blacklist": {
                                "type": "object",
                                "properties": {
                                    "cloud_ids": {
                                        "type": "array",
                                        "items": {
                                            "type": "string",
                                        },
                                    }
                                },
                            },
                        },
                    },
                },
            },
            "additionalProperties": False,
        },
    },
    "additionalProperties": False,
}

NFS_SCHEMA = {
    "type": "object",
    "properties": {
        "enable": {"type": "boolean"},
        "new_config_generator_enabled": {"type": "boolean"},
        "sys": copy.deepcopy(SYS_SCHEMA),
        "log": copy.deepcopy(LOG_SCHEMA),
        "domain": {"type": "string"},
        "subdomain": {"type": "string"},
        "names": {"type": "object"},
        "storage": {"type": "object"},
        "diagnostics": {"type": "object"},
        "auth": {"type": "object"},
        "server": {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                },
                "port": {
                    "type": "integer",
                },
            },
        },
        "vhost": {
            "type": "object",
            "properties": {
                "host": {
                    "type": "string",
                },
                "port": {
                    "type": "integer",
                },
            },
        },
        "http_proxy": {
            "type": "object",
            "properties": {
                "port": {
                    "type": "integer",
                },
                "nfs_vhost_host": {
                    "type": "string",
                },
                "nfs_vhost_port": {
                    "type": "integer",
                },
            },
            "additionalProperties": False,
        },
    },
    "additionalProperties": False,
}

SQS_SCHEMA = {
    "type": "object",
    "properties": {
        "enable": {"type": "boolean"},
        "endpoint": {"type": "string"},
        "domain": {"type": "string"},
        "subdomain": {"type": "string"},
        "http_server": {
            "type": "object",
        },
    },
}

SOLOMON_SCHEMA = {
    "type": "object",
    "properties": {
        "enable": {"type": "boolean"},
        "domain": {"type": "string"},
        "subdomain": {"type": "string"},
        "volumes": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "channels_profile": {"type": "integer"},
                    "partitions": {"type": "integer"},
                },
            },
        },
    },
}

CMS_LIMITS_SCHEMA = {
    "type": "object",
    "properties": {
        "disabled_nodes_limit": {"type": "integer"},
        "disabled_nodes_ratio_limit": {"type": "integer"},
    },
}

CMS_SCHEMA = {
    "type": "object",
    "properties": {
        "enable_sentinel": dict(type="boolean"),
        "default_retry_time_sec": {"type": "integer"},
        "default_permission_duration_sec": {"type": "integer"},
        "info_collection_timeout_sec": {"type": "integer"},
        "tenant_limits": copy.deepcopy(CMS_LIMITS_SCHEMA),
        "cluster_limits": copy.deepcopy(CMS_LIMITS_SCHEMA),
        "monitors": {
            "type": "object",
            "properties": {
                "enable_auto_update": {"type": "boolean"},
                "update_interval_sec": {"type": "integer"},
                "ignored_downtime_gap_sec": {"type": "integer"},
                "broken_timeout_min": {"type": "integer"},
                "broken_prep_timeout_min": {"type": "integer"},
                "faulty_prep_timeout_min": {"type": "integer"},
            },
        },
    },
}

STATE_STORAGE = {
    "type": "object",
    "properties": {
        "allow_incorrect": {"type": "boolean"},
        "node_ids": {"type": "array", "items": {"type": "integer"}},
        "node_set": {"type": "array", "items": {"type": "string"}},
    },
}

METERING_SCHEMA = {"type": "object", "properties": {"metering_file": {"type": "string"}}}

YQL_SCHEMA = {
    "type": "object",
    "properties": {
        "endpoint": {"type": "string"},
        "database": {"type": "string"},
        "table_prefix": {"type": "string"},
        "oauth_file": {"type": "string"},
        "ydb_mvp_cloud_endpoint": {"type": "string"},
        "use_iam_from_metadata_service": {"type": "boolean"},
        "use_secure_connection": {"type": "boolean"},
        "use_bearer_for_ydb": {"type": "boolean"},
        "mdb_gateway": {"type": "string"},
        "mdb_transform_host": {"type": "boolean"},
        "object_storage_endpoint": {"type": "string"},
        "task_service_endpoint": {"type": "string"},
        "task_service_database": {"type": "string"},
        "impersonation_service_endpoint": {"type": "string"},
        "secure_task_service": {"type": "boolean"},
        "secure_impersonation_service": {"type": "boolean"},
        "hmac_secret_file": {"type": "string"},
    },
}

YQ_SCHEMA = {
    "type": "object",
    "properties": {
        "enable": {"type": "boolean"},
        "control_plane": {
            "type": "object",
            "properties": {
                "enable": {"type": "boolean"},
                "endpoint": {"type": "string"},
                "database": {"type": "string"},
                "table_prefix": {"type": "string"},
                "oauth_file": {"type": "string"},
                "certificate_file": {"type": "string"},
                "iam_endpoint": {"type": "string"},
                "sa_key_file": {"type": "string"},
                "use_local_metadata_service": {"type": "boolean"},
                "enable_forward_analytics": {"type": "boolean"},
                "enable_permissions": {"type": "boolean"},
            },
        },
        "analytics": {
            "type": "object",
            "properties": {
                "enable": {"type": "boolean"},
                "endpoint": {"type": "string"},
                "database": {"type": "string"},
                "table_prefix": {"type": "string"},
                "oauth_file": {"type": "string"},
                "ydb_mvp_cloud_endpoint": {"type": "string"},
                "use_iam_from_metadata_service": {"type": "boolean"},
                "use_secure_connection": {"type": "boolean"},
                "use_bearer_for_ydb": {"type": "boolean"},
                "mdb_gateway": {"type": "string"},
                "mdb_transform_host": {"type": "boolean"},
                "object_storage_endpoint": {"type": "string"},
                "task_service_endpoint": {"type": "string"},
                "task_service_database": {"type": "string"},
                "impersonation_service_endpoint": {"type": "string"},
                "secure_task_service": {"type": "boolean"},
                "secure_impersonation_service": {"type": "boolean"},
                "hmac_secret_file": {"type": "string"},
            },
        },
        "streaming": {
            "type": "object",
            "properties": {
                "enable": {"type": "boolean"},
                "endpoint": {"type": "string"},
                "database": {"type": "string"},
                "table_prefix": {"type": "string"},
                "oauth_file": {"type": "string"},
                "certificate_file": {"type": "string"},
                "hmac_secret_file": {"type": "string"},
                "iam_endpoint": {"type": "string"},
                "sa_key_file": {"type": "string"},
                "use_local_metadata_service": {"type": "boolean"},
                "checkpointing": {
                    "type": "object",
                    "properties": {
                        "enabled": {"type": "boolean"},
                        "period_millis": {"type": "integer"},
                        "max_in_flight": {"type": "integer"},
                    },
                },
            },
        },
        "token_accessor": {
            "type": "object",
            "properties": {"endpoint": {"type": "string"}, "use_ssl": {"type": "boolean"}},
        },
        "folder_service": {
            "type": "object",
            "properties": {
                "enable": {"type": "boolean"},
                "endpoint": {"type": "string"},
                "path_to_root_ca": {"type": "string"},
            },
        },
    },
}

TEMPLATE_SCHEMA = {
    "type": "object",
    "properties": {
        "enable_cores": dict(type="boolean"),
        "state_storage": copy.deepcopy(STATE_STORAGE),
        "system_tablets": {"type": "object"},
        "forbid_implicit_storage_pools": {"type": "boolean"},
        "use_fixed_tablet_types": {"type": "boolean"},
        "monitoring_address": {"type": "string"},
        "use_console_feature": {"type": "boolean"},
        "storage_config_generation": {"type": "integer"},
        "use_walle": {"type": "boolean"},
        "cloud_mode": {"type": "boolean"},
        "host_configs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "generation": {"type": "integer", "min": 0},
                    "drives": {
                        "type": "array",
                        "items": copy.deepcopy(DRIVE_SCHEMA),
                    },
                },
            },
        },
        "static_bs_group_hosts": {
            "type": "array",
            "items": {
                "type": "string",
            },
        },
        "use_cluster_uuid": {
            "type": "boolean",
        },
        "require_address": {
            "type": "boolean",
        },
        "cluster_uuid": {
            "type": "string",
            "minLength": 1,
        },
        "accepted_cluster_uuids": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "security_settings": {
            "type": "object",
            "properties": {
                "enforce_user_token_requirement": {
                    "type": "boolean",
                },
                "monitoring_allowed_sids": {"type": "array", "items": {"type": "string"}},
                "administration_allowed_sids": {"type": "array", "items": {"type": "string"}},
            },
        },
        "static_erasure": {
            "type": "string",
            "minLength": 1,
            "enum": Erasure.all_erasure_type_names(),
        },
        "fail_domain_type": {
            "type": "string",
            "minLength": 1,
            "enum": FailDomainType.all_fail_domain_type_names(),
        },
        "static_pdisk_type": {
            "type": "string",
            "enum": PDiskCategory.all_pdisk_category_names(),
        },
        "nw_cache_file_path": {
            "type": "string",
        },
        "hosts": {
            "type": "array",
            "items": copy.deepcopy(HOST_SCHEMA),
            "minItems": 1,
            "checkNameServiceDuplicates": True,
        },
        "auth": {"type": "object"},
        "log": copy.deepcopy(LOG_SCHEMA),
        "grpc": {"type": "object"},
        "kqp": copy.deepcopy(KQP_SCHEMA),
        "ic": {"type": "object"},
        "pq": {"type": "object"},
        "storage_pools": {"type": "array", "items": copy.deepcopy(STORAGE_POOL)},
        "profiles": copy.deepcopy(PROFILES),
        "domains": {"type": "array", "items": copy.deepcopy(DOMAIN_SCHEMA), "minItems": 1},
        "nbs": copy.deepcopy(NBS_SCHEMA),
        "nbs_control": copy.deepcopy(NBS_SCHEMA),
        "nfs": copy.deepcopy(NFS_SCHEMA),
        "nfs_control": copy.deepcopy(NFS_SCHEMA),
        "sqs": copy.deepcopy(SQS_SCHEMA),
        "features": copy.deepcopy(FEATURES_SCHEMA),
        "shared_cache": copy.deepcopy(SHARED_CACHE_SCHEMA),
        "sys": copy.deepcopy(SYS_SCHEMA),
        "tracing_config": copy.deepcopy(TRACING_SCHEMA),
        "failure_injection_config": copy.deepcopy(FAILURE_INJECTION_CONFIG_SCHEMA),
        "solomon": copy.deepcopy(SOLOMON_SCHEMA),
        "cms": copy.deepcopy(CMS_SCHEMA),
        "resource_broker": {"type": "object"},
        "state_storages": {
            "type": "array",
            "items": {"type": "object", "properties": {}, "additionalProperties": True},
        },
        "metering": copy.deepcopy(METERING_SCHEMA),
        "yql_analytics": copy.deepcopy(YQL_SCHEMA),
        "yq": copy.deepcopy(YQ_SCHEMA),
    },
    "required": ["static_erasure", "hosts"],
}


def _host_and_ic_port(host):
    return "%s:%s" % (host["name"], str(host.get("ic_port", 19001)))


def checkNameServiceDuplicates(validator, allow_duplicates, instance, schema):
    names = collections.Counter([_host_and_ic_port(host) for host in instance])
    node_ids = collections.Counter([host["node_id"] for host in instance if host.get("node_id")])

    for name, count in names.items():
        if count > 1:
            yield jsonschema.ValidationError(
                "Names of items contains non-unique elements %r: %s. "
                % (
                    instance,
                    name,
                )
            )
    for node_id, count in node_ids.items():
        if count > 1:
            yield jsonschema.ValidationError(
                "NodeId of items contains non-unique elements %r: %s. "
                % (
                    instance,
                    node_id,
                )
            )


_Validator = jsonschema.Draft4Validator
_Validator = jsonschema.validators.extend(
    _Validator,
    {
        "checkNameServiceDuplicates": checkNameServiceDuplicates,
    },
)


class Validator(_Validator):
    def __init__(self, schema):
        format_checker = jsonschema.FormatChecker()
        super(Validator, self).__init__(schema, format_checker=format_checker)


def default_validator():
    return Validator(copy.deepcopy(TEMPLATE_SCHEMA))


def validate(template):
    default_validator().validate(template)
