




## we need to know the set of options that gets dumped into `config.yaml` direcly





## Experiments

- run my build and expect `host_configs` to be generated

---

# Data

## We only run ydb_configure after we receive nodes from ik8s marked with labels

- it means we only prepare `config_cluster_template.yaml`, not the `config.yaml`, when we deploy a new region
- effectively it slows us down because we postpone steps that we COULD do earlier (such as copying `immediate_control_board` into config.yaml)

## `host-configs` is the only known option that is not required in runtime


## Do we allow copying of arbitrary fields (such as immediate_control_board)?

No. Copying is bad because typos can render config:
- either unusable (kikimr does not boot)
- or even more inconvenient, ignored by ydbd entirely
