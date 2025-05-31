# Cluster Configuration DSL

{{ ydb-short-name }} supports a domain-specific language (DSL) for cluster configuration that allows you to apply different configuration parameters to different cluster nodes based on their characteristics. This mechanism is called **selectors**.

## Selectors

Selectors allow you to specify which nodes a particular configuration section should apply to. They are specified using the `selector` field in the configuration.

### Basic Syntax

```yaml
config:
  feature_flags:
    enable_something: true
  selector:
    - node_id: [1, 2, 3]
```

This configuration will apply the `feature_flags` section only to nodes with IDs 1, 2, and 3.

### Selector Types

#### By Node ID

```yaml
selector:
  - node_id: [1, 2, 3]
```

#### By Node Host

```yaml
selector:
  - node_host: ["host1.example.com", "host2.example.com"]
```

#### By Data Center

```yaml
selector:
  - data_center: ["dc1", "dc2"]
```

#### By Rack

```yaml
selector:
  - rack: ["rack1", "rack2"]
```

#### By Node Type

```yaml
selector:
  - node_type: ["storage", "compute"]
```

### Combining Selectors

You can combine multiple selector conditions:

```yaml
selector:
  - node_id: [1, 2]
    data_center: ["dc1"]
```

This will apply to nodes 1 and 2 that are located in data center "dc1".

### Multiple Selector Blocks

You can specify multiple selector blocks for different node groups:

```yaml
config:
  feature_flags:
    enable_feature_a: true
  selector:
    - node_id: [1, 2, 3]

---
config:
  feature_flags:
    enable_feature_b: true
  selector:
    - data_center: ["dc2"]
```

## Labels

Labels provide a way to tag configuration sections and reference them later. They are useful for organizing complex configurations.

### Basic Label Usage

```yaml
config: &storage_config
  feature_flags:
    enable_storage_feature: true
  selector:
    - node_type: ["storage"]

---
config: *storage_config
```

### Label Inheritance

You can use labels to create base configurations and extend them:

```yaml
base_config: &base
  feature_flags:
    enable_basic_feature: true

storage_config:
  <<: *base
  feature_flags:
    enable_storage_feature: true
  selector:
    - node_type: ["storage"]
```

## YAML Tags

{{ ydb-short-name }} supports special YAML tags for advanced configuration manipulation.

### !inherit Tag

The `!inherit` tag allows you to inherit configuration from a parent section while adding or modifying specific parameters:

```yaml
base_config: &base
  feature_flags:
    feature_a: true
    feature_b: false

extended_config:
  config: !inherit
    base: *base
    feature_flags:
      feature_c: true
  selector:
    - node_type: ["compute"]
```

This will result in:
```yaml
feature_flags:
  feature_a: true
  feature_b: false
  feature_c: true
```

### !remove Tag

The `!remove` tag allows you to remove specific configuration parameters:

```yaml
base_config: &base
  feature_flags:
    feature_a: true
    feature_b: true
    feature_c: true

modified_config:
  config: !remove
    base: *base
    feature_flags:
      feature_b: null
  selector:
    - node_type: ["storage"]
```

This will result in:
```yaml
feature_flags:
  feature_a: true
  feature_c: true
```

### !append Tag

The `!append` tag allows you to append values to arrays:

```yaml
base_config: &base
  allowed_hosts: ["host1", "host2"]

extended_config:
  config: !append
    base: *base
    allowed_hosts: ["host3", "host4"]
  selector:
    - data_center: ["dc1"]
```

This will result in:
```yaml
allowed_hosts: ["host1", "host2", "host3", "host4"]
```

## CLI Examples

### Loading Configuration with Selectors

```bash
{{ ydb-cli }} admin config replace --config-file config.yaml
```

### Viewing Current Configuration

```bash
{{ ydb-cli }} admin config fetch
```

### Validating Configuration Before Applying

```bash
{{ ydb-cli }} admin config validate --config-file config.yaml
```

## Best Practices

1. **Use meaningful labels**: Choose descriptive names for your configuration labels to make them easy to understand and maintain.

2. **Group related configurations**: Use selectors to group configurations logically by node type, data center, or other relevant characteristics.

3. **Test configurations**: Always validate your configuration before applying it to production clusters.

4. **Document complex selectors**: Add comments to explain complex selector logic and inheritance patterns.

5. **Use inheritance wisely**: Leverage the `!inherit`, `!remove`, and `!append` tags to avoid configuration duplication while maintaining clarity.

## Example: Complete Configuration with Selectors

```yaml
# Base configuration for all nodes
base_config: &base
  feature_flags:
    enable_basic_logging: true
    enable_metrics: true
  log_config:
    default_level: 3

# Storage node specific configuration
storage_config: &storage
  config: !inherit
    base: *base
    feature_flags:
      enable_storage_optimization: true
    blob_storage_config:
      service_set:
        availability_domains: 3
  selector:
    - node_type: ["storage"]

# Compute node specific configuration
compute_config: &compute
  config: !inherit
    base: *base
    feature_flags:
      enable_compute_optimization: true
  selector:
    - node_type: ["compute"]

# Data center specific overrides
dc1_config:
  config: !inherit
    base: *base
    feature_flags:
      enable_dc1_feature: true
  selector:
    - data_center: ["dc1"]

---
config: *storage

---
config: *compute

---
config: *dc1_config
```

This configuration demonstrates how to use selectors, labels, and YAML tags to create a flexible and maintainable cluster configuration.

{% note info %}

The selector mechanism is supported only for [database nodes](../../../concepts/glossary.md#database-node).

{% endnote %}

## Labels {#labels}

Labels are special tags that can be used to mark nodes or groups of nodes. Each node has a set of automatically assigned labels:

* `node_id` — internal node identifier in the system;
* `node_host` — node `hostname` obtained at startup;
* `tenant` — database served by this node;
* `dynamic` — whether this node is dynamic (true/false).

And, optionally, any additional labels explicitly defined by the user when starting the `ydbd` process on the node using command line arguments. For example: `--label example=test`.

## Selector Usage Example {#selectors-example}

The example below defines a general actor system configuration and configuration for the `large_tenant` tenant. By default, with this configuration, the actor system assumes that the node has 4 cores, and nodes of the `large_tenant` tenant have 16 cores each, while also overriding the node type for the actor system to `COMPUTE`.

```yaml
metadata:
  cluster: ""
  version: 8
config:
  actor_system_config:
    use_auto_config: true
    node_type: STORAGE
    cpu_count: 4

# Section used as a hint when generating possible configurations using the resolve command
allowed_labels:
  dynamic:
    type: string

selector_config:
- description: large_tenant has bigger nodes with 16 cpu # arbitrary description string
  selector: # selector for all nodes of large_tenant tenant
    tenant: large_tenant
  config:
    actor_system_config: !inherit # reuse original actor_system_config, !inherit semantics described in section below
      # in this case !inherit allows managing actor_system_config.use_auto_config parameter simultaneously for the entire cluster by changing only the base setting
      cpu_count: 16
      node_type: COMPUTE
```

## Allowed Labels

A mapping where you can specify allowed values for labels. The section is used as a hint when generating possible configurations using the `resolve` command. Values are not validated when starting nodes.

Two types of labels are available:

* `string`;
* `enum`.

### string

Can take any values or be unset.

Example:

```yaml
dynamic:
  type: string
host_name:
  type: string
```

### enum

Can take values from the `values` list or be unset.

Example:

```yaml
flavour:
  type: enum
  values:
    ? small
    ? medium
    ? big
```

## Selector Behavior

Selectors represent a simple predicate language. Selectors for each label are combined through an **AND** condition.

### Simple Selector

The following selector will select nodes where the value of label `label1` equals `value1` **and** label `label2` equals `value2`:

```yaml
selector:
  label1: value1
  label2: value2
```

The following selector will select **ALL** nodes in the cluster, since no conditions are specified:

```yaml
selector: {}
```

### In

This operator allows selecting nodes with label values from a list.

The following selector will select all nodes where label `label1` equals `value1` **or** `value2`:

```yaml
selector:
  label1:
    in:
    - value1
    - value2
```

### NotIn

This operator allows selecting nodes where the selected label does not equal a value from the list.

The following selector will select all nodes where label `label1` equals `value1` **and** `label2` does not equal `value2` **and** `value3`:

```yaml
selector:
  label1: value1
  label2:
    not_in:
    - value2
    - value3
```

## Additional YAML Tags

Tags are necessary for partial or complete reuse of configurations from previous selectors. With them, you can combine, extend, delete, and completely override parameters specified in previous selectors and the main configuration.

### !inherit

**Scope:** [YAML mapping](https://yaml.org/spec/1.2.2/#mapping)
**Action:** similar to [merge tag](https://yaml.org/type/merge.html) in YAML, copy all child elements from the parent mapping and merge with current ones with overwriting
**Example:**

#|
|| Initial configuration | Override | Resulting configuration ||
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
**Action:** copy elements from the parent array and overwrite, treating the key object in elements as a key, appending new keys to the end
**Example:**

#|
|| Initial configuration | Override | Resulting configuration ||
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
**Action:** remove element with the corresponding key.
**Example:**

#|
|| Initial configuration | Override | Resulting configuration ||
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
**Action:** copy elements from the parent array and append new ones to the end
**Example:**

#|
|| Initial configuration | Override | Resulting configuration ||
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

## Generating Final Configurations {#selectors-resolve}

Configurations can contain complex sets of overrides. Using [{{ ydb-short-name }} CLI](../../../reference/ydb-cli/index.md), it is possible to view final configurations for:

* specific nodes;
* label sets;
* all possible combinations for the current configuration.

```bash
# Generate all possible final configurations for cluster.yaml
{{ ydb-cli }} admin config resolve --all -f cluster.yaml
# Generate configuration for cluster.yaml with labels tenant=/Root/test and canary=true
{{ ydb-cli }} admin config resolve -f cluster.yaml --label tenant=/Root/test --label canary=true
# Generate configuration for cluster.yaml with labels similar to current ones on node 1001
{{ ydb-cli }} admin config resolve -f cluster.yaml --node_id 1001
# Take current cluster configuration and generate final configuration for it with labels similar to current ones on node 1001
{{ ydb-cli }} admin config resolve --from-cluster --node_id 1001
```

The configuration transformation command is described in more detail in the [{#T}](../../../reference/ydb-cli/configs.md) section.

Example output of `{{ ydb-cli }} admin config resolve --all -f cluster.yaml` for the following configuration file:

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
    actor_system_config: !inherit # reuse original actor_system_config, !inherit semantics described in section below
      node_type: COMPUTE
      cpu_count: 8
```

Output:

```yaml
---
label_sets: # label sets for which configuration was generated
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