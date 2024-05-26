# Cluster configuration DSL

## Selectors {#selectors-intro}

The main entity of the DSL is **selectors**. They allow the overriding of parts of the configuration or the entire configuration for specific nodes or groups of nodes. For example, they can be used to enable experimental functionality for nodes of a particular database. Each selector is an array of overrides and extensions to the main configuration. Each selector has a `description` field, which can be used to store an arbitrary description string. The `selector` field represents a set of rules that determine whether the selector should be applied to a specific node based on a set of labels. The `config` field describes the override rules. Selectors are applied in the order they are described.

## Labels {#labels}

Labels are special tags used to mark nodes or groups of nodes. Each node has a set of automatically assigned labels:
* `node_id` — the internal identifier of the node in the system
* `node_host` — the node's `hostname` obtained at startup
* `tenant` — the database served by this node
* `dynamic` — whether this node is dynamic (true/false)

Additionally, the user can explicitly define any additional labels when starting the `ydbd` process on the node using command-line arguments, such as `--label example=test`.

## Example of using selectors {#selectors-example}

The example below defines the actor system's general configuration and the tenant `large_tenant` configuration. By default, with such a configuration, the actor system assumes that each node has 4 cores, while nodes of the `large_tenant` have 16 cores. The actor system's node type is overridden to `COMPUTE`.

```yaml
metadata:
  cluster: ""
  version: 8
config:
  actor_system_config:
    use_auto_config: true
    node_type: STORAGE
    cpu_count: 4

# This section is used as a hint when generating possible configurations using the resolve command
allowed_labels:
  dynamic:
    type: string

selector_config:
- description: large_tenant has bigger nodes with 16 cpu # arbitrary description string
  selector: # selector for all nodes of the tenant large_tenant
    tenant: large_tenant
  config:
    actor_system_config: !inherit # reuse the original actor_system_config, the semantics of !inherit are described in the section below
      # in this case, !inherit allows managing the actor_system_config.use_auto_config parameter for the entire cluster by changing only the base setting
      cpu_count: 16
      node_type: COMPUTE
```

## Permissive labels

A mapping in which you can set the allowable values for labels. This section is used as a hint when generating possible configurations using the resolve command. Values are not validated at node startup.

There are two types of labels available:
* string;
* enum.

### string
It can take any value or be unset.

Example:
```yaml
dynamic:
  type: string
host_name:
  type: string
```

### enum
It can take values from the `values` list or be unset.

Example:
```yaml
flavour:
  type: enum
  values:
    ? small
    ? medium
    ? big
```

## Selector behavior

Selectors represent a simple predicate language. Selectors for each label are combined using the **AND** condition.

### Simple selector

The following selector will select nodes where the `label1` is equal to `value1` **and** the `label2` is equal to `value2`:
```yaml
selector:
  label1: value1
  label2: value2
```

The following selector will select **ALL** nodes in the cluster, as no conditions are specified:
```yaml
selector: {}
```

### In

This operator allows for selecting nodes with label values from a list.

The following selector will select all nodes where `label1` is equal to `value1` **or** `value2`:
```yaml
selector:
  label1:
    in:
    - value1
    - value2
```

### NotIn

This operator allows selecting nodes where the chosen label does not match any value from a list.

The following selector will select all nodes where `label1` is equal to `value1` **and** `label2` is not equal to `value2` **and** `value3`:
```yaml
selector:
  label1: value1
  label2:
    not_in:
    - value2
    - value3
```

## Additional YAML tags

Tags are necessary for partial or complete reuse of configurations from previous selectors. They allow you to merge, extend, delete, and override parameters set in previous selectors and the main configuration.

### !inherit

**Scope:** [YAML mapping](https://yaml.org/spec/1.2.2/#mapping)
**Action:** similar to the [merge tag](https://yaml.org/type/merge.html) in YAML, copy all child elements from the parent mapping and merge with the current ones, overwriting them.
**Example:**

#|
|| Original configuration | Override | Resulting configuration ||
||
```yaml
config:
  some_config:
    first_entry: 1
    second_entry: 2
    third_entry: 3
```
|
```yaml
config:
  some_config: !inherit
    second_entry: 100
```
|
```yaml
config:
  some_config:
    first_entry: 1
    second_entry: 100
    third_entry: 3
```
||
|#

### !inherit:\<key\>

**Scope:** [YAML sequence](https://yaml.org/spec/1.2.2/#sequence)
**Action:** copy elements from the parent array and overwrite, treating the `key` object in the elements as the key, appending new keys to the end.
**Example:**

#|
|| Original configuration | Override | Resulting configuration ||
||
```yaml
config:
  some_config:
    array:
    - abc: 2
      value: 10
    - abc: 1
      value: 20
      another_value: test
```
|
```yaml
config:
  some_config: !inherit
    array: !inherit:abc
    - abc: 1
      value: 30
    - abc: 3
      value: 40
```
|
```yaml
config:
  some_config:
    array:
    - abc: 2
      value: 10
    - abc: 1
      value: 30
    - abc: 3
      value: 40
```
||
|#

### !remove

**Scope:** YAML sequence element under `!inherit:<key>`
**Action:** remove the element with the corresponding key.
**Example:**

#|
|| Original configuration | Override | Resulting configuration ||
||
```yaml
config:
  some_config:
    array:
    - abc: 2
      value: 10
    - abc: 1
      value: 20
      another_value: test
```
|
```yaml
config:
  some_config: !inherit
    array: !inherit:abc
    - !remove
      abc: 1
```
|
```yaml
config:
  some_config:
    array:
    - abc: 2
      value: 10
```
||
|#
### !append

**Scope:** [YAML sequence](https://yaml.org/spec/1.2.2/#sequence)
**Action:** copy elements from the parent array and append new ones to the end.
**Example:**

#|
|| Original configuration | Override | Resulting configuration ||
||
```yaml
config:
  some_config:
    array:
    - abc: 2
      value: 10
    - abc: 1
      value: 20
      another_value: test
```
|
```yaml
config:
  some_config: !inherit
    array: !append
    - abc: 1
      value: 30
    - abc: 3
      value: 40
```
|
```yaml
config:
  some_config:
    array:
    - abc: 2
      value: 10
    - abc: 1
      value: 20
      another_value: test
    - abc: 1
      value: 30
    - abc: 3
      value: 40
```
||
|#

## Generating final configurations {#selectors-resolve}

Configurations can contain complex sets of overrides. With the [{{ ydb-short-name }} CLI](../../reference/ydb-cli/index.md), you can view the final configurations for:
* specific nodes
* sets of labels
* all possible combinations for the current configuration

```bash
# Generate all possible final configurations for cluster.yaml
{{ ydb-cli }} admin config resolve --all -f cluster.yaml
# Generate the configuration for cluster.yaml with labels tenant=/Root/test and canary=true
{{ ydb-cli }} admin config resolve -f cluster.yaml --label tenant=/Root/test --label canary=true
# Generate the configuration for cluster.yaml with labels similar to those on node 1001
{{ ydb-cli }} admin config resolve -f cluster.yaml --node_id 1001
# Take the current cluster configuration and generate the final configuration for it with labels similar to those on node 1001
{{ ydb-cli }} admin config resolve --from-cluster --node_id 1001
```

The configuration transformation command is described in more detail in the section [{#T}](../../reference/ydb-cli/configs.md).

Example output of `{{ ydb-cli }} admin config

 resolve --all -f cluster.yaml` for the following configuration file:
```yaml
metadata:
  cluster: ""
  version: 8
config:
  actor_system_config:
    use_auto_config: true
    node_type: STORAGE
    cpu_count: 4
allowed_labels:
  dynamic:
    type: string
selector_config:
- description: Actorsystem for dynnodes # arbitrary description string
  selector: # selector for all nodes with label dynamic = true
    dynamic: true
  config:
    actor_system_config: !inherit # reuse the original actor_system_config, the semantics of !inherit are described in the section below
      node_type: COMPUTE
      cpu_count: 8
```
Output:
```yaml
---
label_sets: # sets of labels for which the configuration is generated
- dynamic:
    type: NOT_SET # one of three label types: NOT_SET | COMMON | EMPTY
config: # generated configuration
  invalid: 1
  actor_system_config:
    use_auto_config: true
    node_type: STORAGE
    cpu_count: 4
---
label_sets:
- dynamic:
    type: COMMON
    value: true # label value
config:
  invalid: 1
  actor_system_config:
    use_auto_config: true
    node_type: COMPUTE
    cpu_count: 8
```