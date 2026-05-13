from scheme.common import optional, with_default, list_with, object_with, object_with_additional_fields, erasure_type, sector_map_profile, logging_levels, deploy_flags


scheme = {
    "__type__": dict,
    "hosts": optional(list_with(str)),
    "disks": with_default(list_with(object_with(
        hosts=list_with(str),
        disks_for_split=with_default(list_with(object_with(partlabel=str, device=str)), []),
        disks_for_use=with_default(list_with(object_with(partlabel=str)), []),
        sector_map=with_default(
            object_with(
                use=with_default(("always", "never", "if_needed"), default="always"),
                profile=with_default(sector_map_profile, "NONE"),
            ),
            default={"use": "always", "profile": "NONE"},
        ),
    )), []),

    "affinity": optional(str),
    "nodes_per_host": with_default(int, 1),
    "freehost": optional(str),
    "erasure": erasure_type,
    "node_layout": optional(object_with(
        num_datacenters=with_default(int, 3),
        nodes_per_rack=with_default(int, 1),
        shuffle_locations=with_default(int, 1),
    )),
    "overridden_configs": optional(object_with_additional_fields()),

    "use_home_dir": with_default(bool, False),
    'deploy_flags': optional(list_with(deploy_flags)),
    "build_args": with_default(list_with(str), ["-r"]),
    "required_cli_args": optional(list_with(str)),
    "rejected_cli_args": optional(list_with(str)),
}
