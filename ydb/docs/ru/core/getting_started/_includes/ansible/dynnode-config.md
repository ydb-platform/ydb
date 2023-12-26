```yaml
static_erasure: none
host_configs:
- drive:
{% for disk_name in disk_info.stdout_lines %}
    - path: "/dev/disk/by-id/{{ disk_name }}"
      type: SSD
{% endfor %}
  host_config_id: 1
hosts:
- host: "{{ hostname.stdout }}"
  host_config_id: 1
  port: 19001
  walle_location:
    body: 1
    data_center: '1'
    rack: '1'
domains_config:
  domain:
  - name: Root
    storage_pool_types:
    - kind: ssd
      pool_config:
        box_id: 1
        erasure_species: none
        kind: ssd
        pdisk_filter:
        - property:
          - type: SSD
        vdisk_kind: Default
  state_storage:
  - ring:
      node:
      - 1
      nto_select: 1
    ssid: 1
actor_system_config:
  executor:
  - {name: System, spin_threshold: '10', threads: 10, max_threads: 14, type: BASIC}
  - {name: User, spin_threshold: '1', threads: 1, max_threads: 1, type: BASIC}
  - {name: Batch, spin_threshold: '1', threads: 2, max_threads: 7, type: BASIC}
  - {name: IO, threads: 1, type: IO}
  - {name: IC, spin_threshold: '10', threads: 4, max_threads: 14, time_per_mailbox_micro_secs: 100, max_avg_ping_deviation: 500, type: BASIC}
  scheduler: {progress_threshold: '10000', resolution: '64', spin_threshold: '0'}
  sys_executor: 0
  user_executor: 1
  batch_executor: 2
  io_executor: 3
  service_executor:
  - {executor_id: 4, service_name: Interconnect}

blob_storage_config:
  service_set:
    groups:
    - erasure_species: none
      rings:
      - fail_domains:
        - vdisk_locations:
          - node_id: 1
            path: "/dev/disk/by-id/{{ disk_info.stdout_lines[0] }}"
            pdisk_category: SSD

channel_profile_config:
  profile:
  - channel:
    - erasure_species: none
      pdisk_category: 0
      storage_pool_kind: ssd
    - erasure_species: none
      pdisk_category: 0
      storage_pool_kind: ssd
    - erasure_species: none
      pdisk_category: 0
      storage_pool_kind: ssd
    profile_id: 0

grpc_config:
  host: {{ inner_net }}

monitoring_config:
  monitoring_address: {{ inner_net }}
```