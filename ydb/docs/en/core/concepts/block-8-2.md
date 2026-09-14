# block-8-2 storage groups

`block-8-2` encodes a blob into eight data parts and two parity parts. A group contains twelve VDisks: ten main positions and two handoff positions. Each VDisk occupies a different failure domain in one failure realm. The minimum is twelve independent failure domains; additional spare domains and free slots must be chosen for the deployment's recovery requirements. No universal recommendation of thirteen or fourteen domains is implied.

Any two failure domains can be unavailable while the group continues reading and writing. With three unavailable domains, availability is not guaranteed. The ideal coding overhead is 1.25 (10/8); handoffs, metadata, fragmentation and transient donor copies require additional space.

## Configuration and version prerequisites

Every storage, controller and database process must use a build that supports `block-8-2` (numeric species 19). Select and pin that build before creating the cluster or pool. Automatic mixed-version negotiation and downgrade to builds without this scheme are not supported.

For a new cluster, set `erasure: block-8-2` in the main configuration, use rack failure domains and provide at least twelve independently located storage nodes. The example shipped with the same source version is `ydb/deploy/yaml_config_examples/block-8-2.yaml`. StateStorage membership is configured independently of the twelve-VDisk storage group.

The species and geometry of an existing static group cannot be changed in place. Dynamic storage pools have their own `erasure_species`; `block-4-2` and `block-8-2` pools can coexist without changing the static group's species. Existing groups cannot be converted by editing their erasure field. Block82 blobs use the headerless format; enabling the general blob-header option does not add headers to this species.

## Maintenance and status

For one unavailable VDisk the group is PARTIAL; for two it is DEGRADED; for three it is DISINTEGRATED. Health check and Viewer report yellow, orange and red respectively; a single starting or replicating VDisk can be blue. Layout and space problems can raise the status independently.

CMS maximum-availability mode permits at most one unavailable or locked VDisk in each affected group. Keep-available mode permits at most two. Already unavailable VDisks count against that allowance. Use CMS permissions before maintenance and wait for recovery before the next operation. Static group reassign and self-heal use distconf; static groups are not targets of ordinary cluster balancing or the dynamic-group layout sanitizer.

`ydb-dstool group list --include-static` lists static groups alongside dynamic pools. Static metadata and `ViewerState` come from Viewer; `OperatingStatus` remains the BSC status of dynamic groups.

## Capacity metadata

BSC exports two dimensionless gauges under `counters=storage_pool_stat,subsystem=erasureMapping`:

- `GroupErasureInfo{group,storagePool,erasureSpecies}=1` for each materialized group. The `group` value uses decimal formatting with a minimum width of nine digits, exactly as in `DskUsedBytes`. Static group 0 uses `group="000000000",storagePool="static"`, without a fabricated dynamic storage pool ID.
- `StoragePoolErasureInfo{storagePoolId,storagePool,erasureSpecies}=1` for each configured pool, including pools with zero materialized groups. `storagePoolId` is `boxId:poolId`.

There is one mapping per group; renaming or deleting a pool/group removes the old series. Unknown or inconsistent metadata is omitted. Consumers must reject missing or ambiguous mappings instead of assuming `block-4-2`.

`DskUsedBytes` still reports physical bytes used by a VDisk on its PDisk, including storage overhead; no erasure label is added to it. Aggregate usage by `(group, storagePool)` and join the separate mapping before applying a scheme-specific capacity model. Do not interpret raw bytes multiplied by 8/10 as an exact SQL data size. BSC group resource estimates use a per-slot factor of `12 * 8 / 10 = 9.6` for Block82; the ideal blob coding overhead and this group factor describe different quantities.

The local fixture at `ydb/deploy/local/block-8-2` exercises static Block82 with separate Block42 and Block82 database pools. External deployment integrations and production rollout policy must be validated separately.
