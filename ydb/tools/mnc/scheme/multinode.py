import ydb.tools.mnc.scheme.common as c


scheme = {
    "__type__": dict,
    "__name__": "multinode",
    "hosts": c.list_with(str),
    "exclude_hosts": c.optional(c.list_with(str)),
    "disks": c.with_default(c.list_with(c.object_with(
        hosts=c.list_with(str),
        disks_for_split=c.with_default(c.list_with(c.object_with(partlabel=str, device=str)), []),
        disks_for_use=c.with_default(c.list_with(c.object_with(partlabel=str)), []),
    )), []),
    "affinity": c.optional(str),
    "disk_size": c.with_default(int, 100),
    "user": c.with_default(str, 'ydb'),
    "nodes_per_host": c.with_default(int, 1),
    "freehost": c.optional(str),
    "erasure": c.erasure_type,
    'pile_count': c.with_default(int, 1),
    "device_type": c.with_default(c.device_type, default="SSD"),
    "sector_map": c.with_default(
        c.object_with(
            use=c.with_default(("always", "never", "if_needed"), default="always"),
            profile=c.with_default(c.sector_map_profile, "NONE"),
        ),
        default={"use": "always", "profile": "NONE"},
    ),
    "node_layout": c.optional(c.object_with(
        piles=c.optional(int),
        datacenters_per_pile=c.with_default(int, 1),
        num_datacenters=c.optional(int),
        nodes_per_rack=c.with_default(int, 1),
        shuffle_locations=c.with_default(int, 1),
        _one_of__=[
            ["piles", "num_datacenters"],
        ],
    )),
    "storage_pools": c.optional(c.list_with(c.object_with(name=str, storage_group_count=c.with_default(int, 1)))),
    "domain": c.optional(c.object_with(
        name=c.with_default(str, 'Root'),
        databases=c.list_with(c.object_with(
            name=str,
            storage_group_count=int,
            compute_unit_count=int,
            overridden_configs=c.optional(c.object_with_additional_fields()),
        )),
    )),
    "overridden_configs": c.optional(c.object_with_additional_fields()),
    "build_args": c.with_default(c.list_with(str), ["-r"]),
    "log": c.optional(c.object_with(
        global_level=c.logging_levels,
        entries=c.optional(c.list_with(c.object_with(
            name=str,
            level=c.logging_levels,
        ))),
    )),
    "use_nw_cache": c.with_default(bool, False),
    "with_nbs": c.with_default(bool, False),
    "use_home_dir": c.with_default(bool, False),
    "deploy_flags": c.optional(c.list_with(c.deploy_flags)),
    "required_cli_args": c.optional(c.list_with(str)),
    "rejected_cli_args": c.optional(c.list_with(str)),
    "ports": c.optional(c.object_with(
        ic=c.range_list,
        http=c.range_list,
        grpc=c.range_list,
    )),
    "__one_of__": [
        c.optional(["custom_multinode_path", "use_home_dir"]),
    ],
}
