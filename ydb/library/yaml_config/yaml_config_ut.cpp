#include "yaml_config.h"

#include <ydb/library/yaml_config/yaml_config_parser.h>
#include <ydb/library/yaml_config/public/yaml_config_impl.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/key.pb.h>

#include <util/string/strip.h>

using namespace NKikimr;

const char *WholeConfig = R"(---
cluster: test
version: 12.1
config: &default_config
  log_config: &default_log_config
    entry:
    - component: ZERO
      level: -1
    - component: TEST
      level: -1
  actor_system_config:
    executor:
    - type: BASIC
      threads: 9
      spin_threshold: 1
      name: System
    - type: BASIC
      threads: 16
      spin_threshold: 1
      name: User
    - type: BASIC
      threads: 7
      spin_threshold: 1
      name: Batch
    - type: IO
      threads: 1
      name: IO
    - type: BASIC
      threads: 3
      spin_threshold: 10
      name: IC
      time_per_mailbox_micro_secs: 100
    scheduler:
      resolution: 64
      spin_threshold: 0
      progress_threshold: 10000
    sys_executor: 0
    user_executor: 1
    io_executor: 3
    batch_executor: 2
    service_executor:
    - service_name: Interconnect
      executor_id: 4

allowed_labels:
  tenant:
    type: string
  flavour:
    type: enum
    values:
      ? small
      ? medium
      ? big

selector_config:
- description: set BLOB_DEPOT level to 1
  selector:
    tenant: /ru/tenant2
  config: !inherit
    new_item: 1
- description: set initial log config
  selector:
    tenant: /ru/tenant1
  config: !inherit
    log_config:
      entry: !inherit:component
      - component: ZERO
        level: 7
      - component: BLOB_DEPOT
        level: 0
      - component: TEST
        level: 11
- description: set BLOB_DEPOT level to 1
  selector:
    tenant: /ru/tenant1
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: BLOB_DEPOT
        level: 1
- description: set BLOB_DEPOT level to 2
  selector:
    tenant: /ru/tenant2
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: BLOB_DEPOT
        level: 2
- description: set TEST level to 111
  selector:
    tenant: /ru/tenant1
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: TEST
        level: 111
- description: set TEST level to 112
  selector:
    tenant:
      in:
      - /ru/tenant1
      - /ru/tenant3
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: TEST
        level: 112
      default: 10
- description: set TEST level to 113
  selector:
    tenant:
      not_in:
        - /ru/tenant1
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: TEST
        level: 113
- description: set ACTOR level to 16
  selector:
    tenant: /ru/tenant1
    flavour: small
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: ACTOR
        level: 16
- description: set ACTOR level to 17
  selector:
    flavour: big
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: ACTOR
        level: 17
- description: add add to ACTOR
  selector:
    flavour: small
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - !inherit
        component: ZERO
        add: test
- description: rewrite ZERO to empty
  selector:
    flavour: medium
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - component: ZERO
- description: rewrite ZERO to empty
  selector:
    tenant: /ru/tenant1
    flavour: medium
  config: !inherit
    log_config: !inherit
      entry: !inherit:component
      - !remove
        component: ZERO
      - component: BLOB_DEPOT
        level: 2
      - component: BLOB_DEPOT
        level: 3
)";


const char *BigTenant1Config = R"(---
log_config:
  entry:
  - component: ZERO
    level: 7
  - component: BLOB_DEPOT
    level: 1
  - component: TEST
    level: 112
  - component: ACTOR
    level: 17
  default: 10
actor_system_config:
  executor:
  - type: BASIC
    threads: 9
    spin_threshold: 1
    name: System
  - type: BASIC
    threads: 16
    spin_threshold: 1
    name: User
  - type: BASIC
    threads: 7
    spin_threshold: 1
    name: Batch
  - type: IO
    threads: 1
    name: IO
  - type: BASIC
    threads: 3
    spin_threshold: 10
    name: IC
    time_per_mailbox_micro_secs: 100
  scheduler:
    resolution: 64
    spin_threshold: 0
    progress_threshold: 10000
  sys_executor: 0
  user_executor: 1
  io_executor: 3
  batch_executor: 2
  service_executor:
  - service_name: Interconnect
    executor_id: 4
)";

const char *Tenant2Config = R"(---
log_config: &default_log_config
  entry:
  - component: ZERO
    level: -1
  - component: TEST
    level: 113
  - component: BLOB_DEPOT
    level: 2
actor_system_config:
  executor:
  - type: BASIC
    threads: 9
    spin_threshold: 1
    name: System
  - type: BASIC
    threads: 16
    spin_threshold: 1
    name: User
  - type: BASIC
    threads: 7
    spin_threshold: 1
    name: Batch
  - type: IO
    threads: 1
    name: IO
  - type: BASIC
    threads: 3
    spin_threshold: 10
    name: IC
    time_per_mailbox_micro_secs: 100
  scheduler:
    resolution: 64
    spin_threshold: 0
    progress_threshold: 10000
  sys_executor: 0
  user_executor: 1
  io_executor: 3
  batch_executor: 2
  service_executor:
  - service_name: Interconnect
    executor_id: 4
new_item: 1
)";

const char *SmallConfig = R"(---
log_config: &default_log_config
  entry:
  - component: ZERO
    level: -1
    add: test
  - component: TEST
    level: 113
actor_system_config:
  executor:
  - type: BASIC
    threads: 9
    spin_threshold: 1
    name: System
  - type: BASIC
    threads: 16
    spin_threshold: 1
    name: User
  - type: BASIC
    threads: 7
    spin_threshold: 1
    name: Batch
  - type: IO
    threads: 1
    name: IO
  - type: BASIC
    threads: 3
    spin_threshold: 10
    name: IC
    time_per_mailbox_micro_secs: 100
  scheduler:
    resolution: 64
    spin_threshold: 0
    progress_threshold: 10000
  sys_executor: 0
  user_executor: 1
  io_executor: 3
  batch_executor: 2
  service_executor:
  - service_name: Interconnect
    executor_id: 4
)";

const char *MediumConfig = R"(---
log_config: &default_log_config
  entry:
  - component: ZERO
  - component: TEST
    level: 113
actor_system_config:
  executor:
  - type: BASIC
    threads: 9
    spin_threshold: 1
    name: System
  - type: BASIC
    threads: 16
    spin_threshold: 1
    name: User
  - type: BASIC
    threads: 7
    spin_threshold: 1
    name: Batch
  - type: IO
    threads: 1
    name: IO
  - type: BASIC
    threads: 3
    spin_threshold: 10
    name: IC
    time_per_mailbox_micro_secs: 100
  scheduler:
    resolution: 64
    spin_threshold: 0
    progress_threshold: 10000
  sys_executor: 0
  user_executor: 1
  io_executor: 3
  batch_executor: 2
  service_executor:
  - service_name: Interconnect
    executor_id: 4
)";

const char *MediumTenant1Config = R"(---
log_config: &default_log_config
  entry:
  - component: BLOB_DEPOT
    level: 3
  - component: TEST
    level: 112
  default: 10
actor_system_config:
  executor:
  - type: BASIC
    threads: 9
    spin_threshold: 1
    name: System
  - type: BASIC
    threads: 16
    spin_threshold: 1
    name: User
  - type: BASIC
    threads: 7
    spin_threshold: 1
    name: Batch
  - type: IO
    threads: 1
    name: IO
  - type: BASIC
    threads: 3
    spin_threshold: 10
    name: IC
    time_per_mailbox_micro_secs: 100
  scheduler:
    resolution: 64
    spin_threshold: 0
    progress_threshold: 10000
  sys_executor: 0
  user_executor: 1
  io_executor: 3
  batch_executor: 2
  service_executor:
  - service_name: Interconnect
    executor_id: 4
)";

const char *UnresolvedSimpleConfig1 = R"(---
cluster: test
version: 12.1
config:
  num:
    ? 0
allowed_labels:
  tenant:
    type: string
incompatibility_overrides:
  disable_rules:
    - builtin_branch_must_have_value
    - builtin_dynamic_must_have_value
    - builtin_node_host_must_have_value
    - builtin_node_id_must_have_value
    - builtin_rev_must_have_value
    - builtin_node_type_must_be_defined
    - builtin_tenant_must_be_defined
selector_config:
- description: 1
  selector:
    tenant: abc
  config: !inherit
    num: !inherit
      ? 1
)";

const char *ResolvedSimpleConfig1Negative = R"(---
config:
  num:
    ? 0
)";

const char *ResolvedSimpleConfig1Empty = R"(---
config:
  num:
    ? 0
)";

const char *ResolvedSimpleConfig1Abc = R"(---
config:
  num:
    ? 0
    ? 1
)";

const char *UnresolvedSimpleConfig2 = R"(---
cluster: test
version: 12.1
config:
  num:
    ? 0
allowed_labels:
  tenant:
    type: string
incompatibility_overrides:
  disable_rules:
    - builtin_branch_must_have_value
    - builtin_dynamic_must_have_value
    - builtin_node_host_must_have_value
    - builtin_node_id_must_have_value
    - builtin_rev_must_have_value
    - builtin_node_type_must_be_defined
    - builtin_tenant_must_be_defined
selector_config:
- description: 1
  selector:
    tenant: abc
  config: !inherit
    num: !inherit
      ? 1
- description: 2
  selector:
    tenant: ""
  config: !inherit
    num: !inherit
      ? 2
)";

const char *ResolvedSimpleConfig2Negative = R"(---
config:
  num:
    ? 0
)";

const char *ResolvedSimpleConfig2Empty = R"(---
config:
  num:
    ? 0
    ? 2
)";

const char *ResolvedSimpleConfig2Abc = R"(---
config:
  num:
    ? 0
    ? 1
)";

const char *UnresolvedSimpleConfig3 = R"(---
cluster: test
version: 12.1
config:
  num:
    ? 0
allowed_labels:
  tenant:
    type: string
incompatibility_overrides:
  disable_rules:
    - builtin_branch_must_have_value
    - builtin_dynamic_must_have_value
    - builtin_node_host_must_have_value
    - builtin_node_id_must_have_value
    - builtin_rev_must_have_value
    - builtin_node_type_must_be_defined
    - builtin_tenant_must_be_defined
selector_config:
- description: 1
  selector:
    tenant:
      not_in:
        - abc
  config: !inherit
    num: !inherit
      ? 1
)";

const char *ResolvedSimpleConfig3Negative = R"(---
config:
  num:
    ? 0
    ? 1
)";

const char *ResolvedSimpleConfig3Empty = R"(---
config:
  num:
    ? 0
    ? 1
)";

const char *ResolvedSimpleConfig3Abc = R"(---
config:
  num:
    ? 0
)";

const char *UnresolvedAllConfig = R"(---
cluster: test
version: 12.1
config:
  num:
    ? 0
incompatibility_overrides:
  disable_rules:
    - builtin_branch_must_have_value
    - builtin_dynamic_must_have_value
    - builtin_node_host_must_have_value
    - builtin_node_id_must_have_value
    - builtin_rev_must_have_value
    - builtin_node_type_must_be_defined
    - builtin_tenant_must_be_defined
allowed_labels:
  tenant:
    type: string
  flavour:
    type: enum
    values:
      ? def
      ? jkl
  kind:
    type: enum
    values:
      ? mno
      ? pqr
  type:
    type: enum
    values:
      ? foo

selector_config:
- description: 1
  selector:
    tenant: abc
  config: !inherit
    num: !inherit
      ? 1
- description: 2
  selector:
    flavour: def
  config: !inherit
    num: !inherit
      ? 2
- description: 3
  selector:
    tenant: abc
    flavour: def
  config:
    num:
      ? 3
- description: 4
  selector:
    tenant: abc
  config: !inherit
    num: !inherit
      ? 4
- description: 5
  selector:
    tenant: ghi
  config: !inherit
    num: !inherit
      ? 5
- description: 6
  selector:
    flavour: jkl
  config: !inherit
    num: !inherit
      ? 6
- description: 7
  selector:
    kind: mno
  config: !inherit
    num: !inherit
      ? 7
- description: 8
  selector:
    kind: pqr
  config: !inherit
    num: !inherit
      ? 8
- description: 9
  selector:
    kind:
      not_in:
      - pqr
  config: !inherit
    num: !inherit
      ? 9
- description: 10
  selector:
    flavour:
      in:
      - def
      - jkl
  config: !inherit
    num: !inherit
      ? 10
- description: 11
  selector:
    flavour:
      not_in:
      - def
    kind: mno
    tenant: abc
  config: !inherit
    num: !inherit
      ? 11
- description: 12
  selector:
    type: foo
  config: !inherit
    num: !inherit
      ? 12
- description: 13
  selector:
    tenant:
      not_in:
      - abc
    type:
      not_in:
      - foo
    flavour: def
    kind: mno
  config: !inherit
    num: !inherit
      ? 13)";

const char *UnresolvedAllConfig14 = R"(---
- description: 14
  selector:
    tenant:
      in:
      - aaa0
      - bbb0
      - ccc0
      - ddd0
      - eee0
      - fff0
      - ggg0
      - kkk0
      - lll0
      - mmm0
      - aaa1
      - bbb1
      - ccc1
      - ddd1
      - eee1
      - fff1
      - ggg1
      - kkk1
      - lll1
      - mmm1
      - aaa2
      - bbb2
      - ccc2
      - ddd2
      - eee2
      - fff2
      - ggg2
      - kkk2
      - lll2
      - mmm2
      - aaa3
      - bbb3
      - ccc3
      - ddd3
      - eee3
      - fff3
      - ggg3
      - kkk3
      - lll3
      - mmm3
      - aaa4
      - bbb4
      - ccc4
      - ddd4
      - eee4
      - fff4
      - ggg4
      - kkk4
      - lll4
      - mmm4
      - aaa5
      - bbb5
      - ccc5
      - ddd5
      - eee5
      - fff5
      - ggg5
      - kkk5
      - lll5
      - mmm5
  config: !inherit
    num: !inherit
      ? 14
)";

const char *SimpleConfig = R"(---
cluster: test
version: 12.1
config:
  num: 1
allowed_labels:
  tenant:
    type: string

selector_config: []
)";

const char *VolatilePart = R"(
- description: test 4
  selector:
    tenant: /dev_global
  config:
    actor_system_config: {}
    cms_config:
      sentinel_config:
        enable: false
- description: test 5
  selector:
    canary: true
  config:
    actor_system_config: {}
    cms_config:
      sentinel_config:
        enable: true
)";

const char *Concatenated = R"(---
cluster: test
version: 12.1
config:
  num: 1
allowed_labels:
  tenant:
    type: string
selector_config:
- description: test 4
  selector:
    tenant: /dev_global
  config:
    actor_system_config: {}
    cms_config:
      sentinel_config:
        enable: false
- description: test 5
  selector:
    canary: true
  config:
    actor_system_config: {}
    cms_config:
      sentinel_config:
        enable: true
)";

const char *UnresolvedSimpleConfigAppend = R"(---
cluster: test
version: 12.1
config:
  num:
  - 0
allowed_labels:
  tenant:
    type: string
incompatibility_overrides:
  disable_rules:
    - builtin_branch_must_have_value
    - builtin_dynamic_must_have_value
    - builtin_node_host_must_have_value
    - builtin_node_id_must_have_value
    - builtin_rev_must_have_value
    - builtin_node_type_must_be_defined
    - builtin_tenant_must_be_defined
selector_config:
- description: 1
  selector:
    tenant: abc
  config: !inherit
    num: !append
    - 0
- description: 2
  selector:
    tenant: ""
  config: !inherit
    num: !append
    - 1
)";

const char *ResolvedSimpleConfigAppendAbc = R"(---
config:
  num:
  - 0
  - 0
)";

const char *ResolvedSimpleConfigAppendEmpty = R"(---
config:
  num:
  - 0
  - 1
)";

using EType = NYamlConfig::TLabel::EType;

TMap<TSet<TVector<NYamlConfig::TLabel>>, TVector<int>> ExpectedResolved =
{
    std::pair{
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (pqr  )  (ghi  )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   5,   6,   8,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (pqr  )  (abc  ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   1,   4,   6,   8,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (mno  )  (abc  )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   1,   4,   6,   7,   9,   10,   11,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (mno  ) !(     ) _(     ) ],
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [ (jkl  )  (mno  ) _(     ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   6,   7,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (mno  ) !(     )  (foo  ) ],
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [ (jkl  )  (mno  ) _(     )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   6,   7,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  ) _(     )  (ghi  )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   5,   6,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  ) _(     )  (ghi  ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   5,   6,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  ) _(     )  (abc  )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   1,   4,   6,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  ) _(     )  (abc  ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   1,   4,   6,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  ) _(     ) !(     )  (foo  ) ],
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [ (jkl  ) _(     ) _(     )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   6,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  ) _(     ) !(     ) _(     ) ],
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [ (jkl  ) _(     ) _(     ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   6,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (mno  )  (abc  )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{3,   4,   7,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (pqr  ) !(     ) _(     ) ],
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [ (def  )  (pqr  ) _(     ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   2,   8,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (mno  )  (abc  ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{3,   4,   7,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (mno  )  (ghi  )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   2,   5,   7,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (mno  )  (ghi  ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   5,   6,   7,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (pqr  )  (ghi  ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   2,   5,   8,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (mno  ) !(     ) _(     ) ],
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [ (def  )  (mno  ) _(     ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   2,   7,   9,   10,   13},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  ) _(     )  (ghi  )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   2,   5,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (pqr  )  (abc  )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   1,   4,   8,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (mno  )  (ghi  ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   2,   5,   7,   9,   10,   13},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  ) _(     )  (abc  )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{3,   4,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     ) _(     ) !(     ) _(     ) ],
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [_(     ) _(     ) _(     ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   9},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  ) _(     )  (ghi  ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   2,   5,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  ) _(     ) !(     )  (foo  ) ],
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [ (def  ) _(     ) _(     )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   2,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (mno  )  (ghi  ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   5,   7,   9},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (pqr  )  (ghi  )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   5,   8,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  ) _(     )  (abc  ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{3,   4,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (pqr  ) !(     )  (foo  ) ],
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [_(     )  (pqr  ) _(     )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   8,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (pqr  )  (abc  ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{3,   4,   8,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (pqr  )  (ghi  ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   5,   6,   8,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (pqr  )  (ghi  ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   5,   8},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (pqr  ) !(     )  (foo  ) ],
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [ (jkl  )  (pqr  ) _(     )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   6,   8,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (pqr  ) !(     )  (foo  ) ],
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [ (def  )  (pqr  ) _(     )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   2,   8,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (mno  ) !(     )  (foo  ) ],
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [ (def  )  (mno  ) _(     )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("mno")}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   2,   7,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (pqr  ) !(     ) _(     ) ],
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [_(     )  (pqr  ) _(     ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   8},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (pqr  ) !(     ) _(     ) ],
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [ (jkl  )  (pqr  ) _(     ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   6,   8,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (mno  )  (ghi  )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   5,   7,   9,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (pqr  )  (abc  ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("pqr")}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   1,   4,   8},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (pqr  )  (abc  )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("pqr")}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   1,   4,   6,   8,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (mno  )  (abc  ) _(     ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   1,   4,   6,   7,   9,   10,   11},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     ) _(     )  (abc  ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   1,   4,   9},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     ) _(     )  (abc  )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   1,   4,   9,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (mno  ) !(     )  (foo  ) ],
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
            // [_(     )  (mno  ) _(     )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   7,   9,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  ) _(     ) !(     ) _(     ) ],
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [ (def  ) _(     ) _(     ) _(     ) ]
            {{EType::Common, TString("def")}, {EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   2,   9,   10},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (jkl  )  (mno  )  (ghi  )  (foo  ) ]
            {{EType::Common, TString("jkl")}, {EType::Common, TString("mno")}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   5,   6,   7,   9,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (mno  )  (abc  )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   1,   4,   7,   9,   11,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     ) _(     )  (ghi  ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("ghi")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   5,   9},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (mno  )  (abc  ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Common, TString("abc")}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   1,   4,   7,   9,   11},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     )  (mno  ) !(     ) _(     ) ],
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Negative, TString()}, {EType::Empty, TString()}},
            // [_(     )  (mno  ) _(     ) _(     ) ]
            {{EType::Empty, TString()}, {EType::Common, TString("mno")}, {EType::Empty, TString()}, {EType::Empty, TString()}},
        },
        TVector<int>{0,   7,   9},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (pqr  )  (abc  )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Common, TString("abc")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{3,   4,   8,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     ) _(     ) !(     )  (foo  ) ],
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("foo")}},
            // [_(     ) _(     ) _(     )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Negative, TString()}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   9,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [ (def  )  (pqr  )  (ghi  )  (foo  ) ]
            {{EType::Common, TString("def")}, {EType::Common, TString("pqr")}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   2,   5,   8,   10,   12},
    },
    {
        TSet<TVector<NYamlConfig::TLabel>>{
            // [_(     ) _(     )  (ghi  )  (foo  ) ]
            {{EType::Empty, TString()}, {EType::Empty, TString()}, {EType::Common, TString("ghi")}, {EType::Common, TString("foo")}},
        },
        TVector<int>{0,   5,   9,   12},
    },
};

Y_UNIT_TEST_SUITE(YamlConfigOpaqueConfigMarker) {

    Y_UNIT_TEST(OpaqueConfigFieldsIncludesExistingOpaqueFields) {
        const auto& fields = NYaml::OpaqueConfigFields();

        bool found = false;
        for (const auto& f : fields) {
            if (f.Name == "private_database_config") {
                found = true;
                break;
            }
        }
        UNIT_ASSERT_C(found,
            "expected 'private_database_config' in OpaqueConfigFields()");
    }

    Y_UNIT_TEST(OpaqueConfigFieldsAreStable) {
        // The list is computed once and cached; consecutive calls must return
        // the same vector by reference.
        const auto& a = NYaml::OpaqueConfigFields();
        const auto& b = NYaml::OpaqueConfigFields();
        UNIT_ASSERT_EQUAL(&a, &b);
    }

    Y_UNIT_TEST(CaptureOpaqueConfigFieldsReplacesMessageSubtreeWithEmptyMap) {
        NJson::TJsonValue cfg(NJson::JSON_MAP);
        auto& sub = cfg["private_database_config"];
        sub.SetType(NJson::JSON_MAP);
        sub["some_unknown_field"] = 42;
        sub["some_unknown_struct"]["field_1"] = 1;
        sub["some_unknown_struct"]["field_2"] = "abc";

        NYaml::CaptureOpaqueConfigFields(cfg);

        UNIT_ASSERT(cfg.Has("private_database_config"));
        const auto& captured = cfg["private_database_config"];
        UNIT_ASSERT_C(captured.IsMap(),
            "private_database_config must remain a map after capture");
        UNIT_ASSERT_C(captured.GetMap().empty(),
            "private_database_config must be replaced with an empty map");
    }

    Y_UNIT_TEST(CaptureOpaqueConfigFieldsLeavesOtherFieldsUntouched) {
        NJson::TJsonValue cfg(NJson::JSON_MAP);
        cfg["private_database_config"]["dropped"] = "value";
        cfg["feature_flags"]["enable_something"] = true;
        cfg["actor_system_config"]["sys_executor"] = 0;

        NYaml::CaptureOpaqueConfigFields(cfg);

        UNIT_ASSERT(cfg["private_database_config"].IsMap());
        UNIT_ASSERT(cfg["private_database_config"].GetMap().empty());

        UNIT_ASSERT(cfg["feature_flags"].IsMap());
        UNIT_ASSERT_EQUAL(cfg["feature_flags"]["enable_something"].GetBoolean(), true);

        UNIT_ASSERT(cfg["actor_system_config"].IsMap());
        UNIT_ASSERT_EQUAL(cfg["actor_system_config"]["sys_executor"].GetInteger(), 0);
    }

    Y_UNIT_TEST(CaptureOpaqueConfigFieldsIsNoopWhenFieldAbsent) {
        NJson::TJsonValue cfg(NJson::JSON_MAP);
        cfg["feature_flags"]["enable_something"] = true;

        NYaml::CaptureOpaqueConfigFields(cfg);

        UNIT_ASSERT(!cfg.Has("private_database_config"));
        UNIT_ASSERT(cfg["feature_flags"].IsMap());
        UNIT_ASSERT_EQUAL(cfg["feature_flags"]["enable_something"].GetBoolean(), true);
    }

    Y_UNIT_TEST(CaptureOpaqueConfigFieldsIsNoopOnNonMap) {
        // Non-map inputs must not crash and must remain unchanged.
        NJson::TJsonValue arr(NJson::JSON_ARRAY);
        arr.AppendValue(1);
        arr.AppendValue(2);
        NYaml::CaptureOpaqueConfigFields(arr);
        UNIT_ASSERT(arr.IsArray());
        UNIT_ASSERT_VALUES_EQUAL(arr.GetArray().size(), 2u);

        NJson::TJsonValue scalar(42);
        NYaml::CaptureOpaqueConfigFields(scalar);
        UNIT_ASSERT_EQUAL(scalar.GetInteger(), 42);
    }

    Y_UNIT_TEST(CaptureOpaqueConfigFieldsHandlesAlreadyEmptyMap) {
        NJson::TJsonValue cfg(NJson::JSON_MAP);
        cfg["private_database_config"].SetType(NJson::JSON_MAP);

        NYaml::CaptureOpaqueConfigFields(cfg);

        UNIT_ASSERT(cfg["private_database_config"].IsMap());
        UNIT_ASSERT(cfg["private_database_config"].GetMap().empty());
    }

    Y_UNIT_TEST(ParseAcceptsUnknownOpaqueFieldSubtree) {
        // End-to-end: the parser must accept unknown content under an
        // opaque top-level field without allow_unknown_fields, because
        // CaptureOpaqueConfigFields() strips the subtree before the
        // proto merge.
        const TString yaml = R"(---
metadata:
  cluster: ""
  version: 0
config:
  private_database_config:
    some_unknown_field: 42
    some_unknown_struct:
      field_1: 1
      field_2: abc
)";
        NKikimrConfig::TAppConfig cfg;
        UNIT_ASSERT_NO_EXCEPTION(cfg = NYaml::Parse(yaml, /*transform=*/ false));
        // The merge stored an empty message for the opaque field; nothing
        // from the YAML sub-tree leaked into the proto.
        UNIT_ASSERT(cfg.HasPrivateDatabaseConfig());
    }
}

Y_UNIT_TEST_SUITE(YamlConfig) {
    Y_UNIT_TEST(CollectLabels) {
        auto doc = NFyaml::TDocument::Parse(WholeConfig);

        auto labels = NYamlConfig::CollectLabels(doc);

        // TODO: forbid enum extension
        TMap<TString, NYamlConfig::TLabelType> expectedLabels = {
            {"flavour", NYamlConfig::TLabelType{NYamlConfig::EYamlConfigLabelTypeClass::Closed, TSet<TString>{"", "big", "medium", "small"}}},
            {"tenant", NYamlConfig::TLabelType{NYamlConfig::EYamlConfigLabelTypeClass::Open, TSet<TString>{/*"-*", */"", "/ru/tenant1", "/ru/tenant2", "/ru/tenant3"}}},
        };

        UNIT_ASSERT_EQUAL(labels, expectedLabels);
    }

    Y_UNIT_TEST(MaterializeSpecificConfig) {
        auto doc = NFyaml::TDocument::Parse(WholeConfig);

        {
            auto expected = NFyaml::TDocument::Parse(BigTenant1Config);

            auto [resolved, node] = NYamlConfig::Resolve(
                doc,
                {
                    NYamlConfig::TNamedLabel{"flavour", "big"},
                    NYamlConfig::TNamedLabel{"tenant", "/ru/tenant1"},
                }
            );

            UNIT_ASSERT(node.DeepEqual(expected.Root()));
        }

        {
            auto expected = NFyaml::TDocument::Parse(Tenant2Config);

            auto [resolved, node] = NYamlConfig::Resolve(
                doc,
                {
                    NYamlConfig::TNamedLabel{"tenant", "/ru/tenant2"},
                }
            );

            UNIT_ASSERT(node.DeepEqual(expected.Root()));
        }

        {
            auto expected = NFyaml::TDocument::Parse(SmallConfig);

            auto [resolved, node] = NYamlConfig::Resolve(
                doc,
                {
                    NYamlConfig::TNamedLabel{"flavour", "small"},
                }
            );

            UNIT_ASSERT(node.DeepEqual(expected.Root()));
        }

        {
            auto expected = NFyaml::TDocument::Parse(MediumConfig);

            auto [resolved, node] = NYamlConfig::Resolve(
                doc,
                {
                    NYamlConfig::TNamedLabel{"flavour", "medium"},
                }
            );

            UNIT_ASSERT(node.DeepEqual(expected.Root()));
        }

        {
            auto expected = NFyaml::TDocument::Parse(MediumTenant1Config);

            auto [resolved, node] = NYamlConfig::Resolve(
                doc,
                {
                    NYamlConfig::TNamedLabel{"flavour", "medium"},
                    NYamlConfig::TNamedLabel{"tenant", "/ru/tenant1"},
                }
            );

            UNIT_ASSERT(node.DeepEqual(expected.Root()));
        }
    }

    Y_UNIT_TEST(MaterializeAllConfigSimple) {
        TVector<TString> expectedLabels{"tenant"};

        {
            auto doc = NFyaml::TDocument::Parse(UnresolvedSimpleConfig1);
            auto resolved = NYamlConfig::ResolveAll(doc);
            UNIT_ASSERT_VALUES_EQUAL(resolved.Labels, expectedLabels);

            auto empty = NFyaml::TDocument::Parse(ResolvedSimpleConfig1Empty);
            auto negative = NFyaml::TDocument::Parse(ResolvedSimpleConfig1Negative);
            auto abc = NFyaml::TDocument::Parse(ResolvedSimpleConfig1Abc);

            TSet<TVector<NYamlConfig::TLabel>> first = {
                {NYamlConfig::TLabel{EType::Empty, ""}},
                {NYamlConfig::TLabel{EType::Negative, ""}},
            };

            auto firstIt = resolved.Configs.find(first);
            UNIT_ASSERT_UNEQUAL(firstIt, resolved.Configs.end());
            UNIT_ASSERT(firstIt->second.second.DeepEqual(empty.Root().Map()["config"]));
            UNIT_ASSERT(firstIt->second.second.DeepEqual(negative.Root().Map()["config"]));

            TSet<TVector<NYamlConfig::TLabel>> second = {
                {NYamlConfig::TLabel{EType::Common, "abc"}},
            };

            auto secondIt = resolved.Configs.find(second);
            UNIT_ASSERT_UNEQUAL(secondIt, resolved.Configs.end());
            UNIT_ASSERT(secondIt->second.second.DeepEqual(abc.Root().Map()["config"]));
        }

        {
            auto doc = NFyaml::TDocument::Parse(UnresolvedSimpleConfig2);
            auto resolved = NYamlConfig::ResolveAll(doc);
            UNIT_ASSERT_VALUES_EQUAL(resolved.Labels, expectedLabels);

            auto empty = NFyaml::TDocument::Parse(ResolvedSimpleConfig2Empty);
            auto negative = NFyaml::TDocument::Parse(ResolvedSimpleConfig2Negative);
            auto abc = NFyaml::TDocument::Parse(ResolvedSimpleConfig2Abc);

            TSet<TVector<NYamlConfig::TLabel>> first = {
                {NYamlConfig::TLabel{EType::Empty, ""}},
            };

            auto firstIt = resolved.Configs.find(first);
            UNIT_ASSERT_UNEQUAL(firstIt, resolved.Configs.end());
            UNIT_ASSERT(firstIt->second.second.DeepEqual(empty.Root().Map()["config"]));

            TSet<TVector<NYamlConfig::TLabel>> second = {
                {NYamlConfig::TLabel{EType::Common, "abc"}},
            };

            auto secondIt = resolved.Configs.find(second);
            UNIT_ASSERT_UNEQUAL(secondIt, resolved.Configs.end());
            UNIT_ASSERT(secondIt->second.second.DeepEqual(abc.Root().Map()["config"]));
        }

        {
            auto doc = NFyaml::TDocument::Parse(UnresolvedSimpleConfig3);
            auto resolved = NYamlConfig::ResolveAll(doc);
            UNIT_ASSERT_VALUES_EQUAL(resolved.Labels, expectedLabels);

            auto empty = NFyaml::TDocument::Parse(ResolvedSimpleConfig3Empty);
            auto negative = NFyaml::TDocument::Parse(ResolvedSimpleConfig3Negative);
            auto abc = NFyaml::TDocument::Parse(ResolvedSimpleConfig3Abc);

            TSet<TVector<NYamlConfig::TLabel>> first = {
                {NYamlConfig::TLabel{EType::Empty, ""}},
                {NYamlConfig::TLabel{EType::Negative, ""}},
            };

            auto firstIt = resolved.Configs.find(first);
            UNIT_ASSERT_UNEQUAL(firstIt, resolved.Configs.end());
            UNIT_ASSERT(firstIt->second.second.DeepEqual(empty.Root().Map()["config"]));
            UNIT_ASSERT(firstIt->second.second.DeepEqual(negative.Root().Map()["config"]));

            TSet<TVector<NYamlConfig::TLabel>> second = {
                {NYamlConfig::TLabel{EType::Common, "abc"}},
            };

            auto secondIt = resolved.Configs.find(second);
            UNIT_ASSERT_UNEQUAL(secondIt, resolved.Configs.end());
            UNIT_ASSERT(secondIt->second.second.DeepEqual(abc.Root().Map()["config"]));
        }

        {
            auto doc = NFyaml::TDocument::Parse(UnresolvedSimpleConfigAppend);
            auto resolved = NYamlConfig::ResolveAll(doc);
            UNIT_ASSERT_VALUES_EQUAL(resolved.Labels, expectedLabels);

            auto empty = NFyaml::TDocument::Parse(ResolvedSimpleConfigAppendEmpty);
            auto abc = NFyaml::TDocument::Parse(ResolvedSimpleConfigAppendAbc);

            TSet<TVector<NYamlConfig::TLabel>> first = {
                {NYamlConfig::TLabel{EType::Empty, ""}},
            };

            auto firstIt = resolved.Configs.find(first);
            UNIT_ASSERT_UNEQUAL(firstIt, resolved.Configs.end());
            UNIT_ASSERT(firstIt->second.second.DeepEqual(empty.Root().Map()["config"]));

            TSet<TVector<NYamlConfig::TLabel>> second = {
                {NYamlConfig::TLabel{EType::Common, "abc"}},
            };

            auto secondIt = resolved.Configs.find(second);
            UNIT_ASSERT_UNEQUAL(secondIt, resolved.Configs.end());
            UNIT_ASSERT(secondIt->second.second.DeepEqual(abc.Root().Map()["config"]));
        }
    }

    Y_UNIT_TEST(MaterializeAllConfigs) {
        TVector<TString> expectedLabels{"flavour", "kind", "tenant", "type"};

        {
            auto doc = NFyaml::TDocument::Parse(UnresolvedAllConfig);
            auto resolved = NYamlConfig::ResolveAll(doc);
            TMap<TSet<TVector<NYamlConfig::TLabel>>, TVector<int>> compacted;
            for(auto& [k, v] : resolved.Configs) {
                TVector<int> nums;
                for (auto& kv: v.second.Map()["num"].Map()) {
                    nums.push_back(std::stoi(kv.Key().Scalar()));
                }
                compacted[k] = nums;
            }

            UNIT_ASSERT_VALUES_EQUAL(resolved.Labels, expectedLabels);
            UNIT_ASSERT_VALUES_EQUAL(compacted.size(), ExpectedResolved.size());
            UNIT_ASSERT_VALUES_EQUAL(compacted, ExpectedResolved);
        }

        {
            auto config = TString(UnresolvedAllConfig) + UnresolvedAllConfig14;
            auto doc = NFyaml::TDocument::Parse(config.c_str());
            auto resolved = NYamlConfig::ResolveAll(doc);

            UNIT_ASSERT_VALUES_EQUAL(resolved.Labels, expectedLabels);
            UNIT_ASSERT_VALUES_EQUAL(resolved.Configs.size(), 72);
            // TODO extend ExpectedResolved manually and compare
        }
    }

    Y_UNIT_TEST(AppendVolatileConfig) {
        auto cfg = NFyaml::TDocument::Parse(SimpleConfig);
        auto volatilePart = NFyaml::TDocument::Parse(VolatilePart);
        NYamlConfig::AppendVolatileConfigs(cfg, volatilePart);
        TStringStream stream;
        stream << cfg;
        UNIT_ASSERT_VALUES_EQUAL(stream.Str(), TString(Concatenated));
    }

    Y_UNIT_TEST(AppendAndResolve) {
        auto cfg = NFyaml::TDocument::Parse(SimpleConfig);
        for (int i = 0; i < 4; ++i) {
            auto volatilePart = NFyaml::TDocument::Parse(VolatilePart);
            NYamlConfig::AppendVolatileConfigs(cfg, volatilePart);
        }
        TStringStream stream;
        stream << cfg;
    }

    void CheckAppendDatabaseConfig(
        const TString mainConfig,
        const TString databaseConfig)
    {
        auto treeOriginal = NFyaml::TDocument::Parse(mainConfig);
        auto tree = NFyaml::TDocument::Parse(mainConfig);
        auto dbTree = NFyaml::TDocument::Parse(databaseConfig);

        const size_t selectorsCountBefore =
            tree.Root().Map().Has("selector_config")
                ? tree.Root().Map().at("selector_config").Sequence().size()
                : 0;

        // Add empty selector_config nodes to avoid processing missing YAML
        // nodes
        if (!tree.Root().Map().Has("selector_config")) {
            tree.Root().Map().Append(
                tree.Buildf("selector_config"),
                tree.Buildf("[]"));
        }
        if (!treeOriginal.Root().Map().Has("selector_config")) {
            treeOriginal.Root().Map().Append(
                treeOriginal.Buildf("selector_config"),
                treeOriginal.Buildf("[]"));
        }

        UNIT_ASSERT_NO_EXCEPTION(
            NKikimr::NYamlConfig::AppendDatabaseConfig(tree, dbTree));

        auto selectors = tree.Root().Map().at("selector_config").Sequence();
        UNIT_ASSERT_VALUES_EQUAL(selectors.size(), selectorsCountBefore + 1);

        auto lastSelector = selectors.at(selectors.size() - 1).Map();
        UNIT_ASSERT(lastSelector.Has("config"));

        // Check that main config selectors have not changed
        auto selectorsOriginal =
            treeOriginal.Root().Map().at("selector_config").Sequence();

        for (size_t i = 0; i < selectorsCountBefore; i++) {
            UNIT_ASSERT_VALUES_EQUAL(
                selectors.at(i).Map().size(),
                selectorsOriginal.at(i).Map().size());

            // Сheck all map elements (description, selector, config)
            for (auto field = selectors.at(i).Map().begin(),
                      fieldOrig = selectorsOriginal.at(i).Map().begin();
                 field != selectors.at(i).Map().end();
                 field++, fieldOrig++)
            {
                TStringStream fieldStr;
                fieldStr << field->Key() << ": " << field->Value();

                TStringStream fieldOriginalStr;
                fieldOriginalStr << fieldOrig->Key() << ": "
                                 << fieldOrig->Value();

                UNIT_ASSERT_VALUES_EQUAL(
                    fieldStr.Str(),
                    fieldOriginalStr.Str());
            }
        }

        // Check that the db config selector is appended to the end of the main
        // config selectors
        TStringStream actualLastConfig;
        actualLastConfig << lastSelector.at("config");

        TStringStream expectedConfig;
        expectedConfig << dbTree.Root().Map().at("config");

        UNIT_ASSERT_VALUES_EQUAL(actualLastConfig.Str(), expectedConfig.Str());
    }

    Y_UNIT_TEST(AppendDatabaseConfigAsTheOnlySelector)
    {
        const TString mainConfig = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  feature_flags:
    some_param_1: true
)";

        const TString databaseConfig = R"(
---
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  feature_flags: !inherit
    some_param_1: false
)";

        CheckAppendDatabaseConfig(mainConfig, databaseConfig);
    }

    Y_UNIT_TEST(AppendDatabaseConfigAfterExistingSelectors)
    {
        const TString mainConfig = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 0
config:
  feature_flags:
    some_param_1: true
selector_config:
- description: pre-existing selector 1 with config
  selector:
    tenant: dynamic
  config:
    feature_flags: !inherit
      some_param_2: true
- description: pre-existing selector 2 with config
  selector:
    tenant: dynamic
  config:
    feature_flags: !inherit
      some_param_3: true
)";

        const TString databaseConfig = R"(
---
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  feature_flags: !inherit
    some_param_1: false
)";

        CheckAppendDatabaseConfig(mainConfig, databaseConfig);
    }

    Y_UNIT_TEST(GetMetadata) {
        {
            TString str = R"(
metadata:
  version: 10
  cluster: foo
)";
            auto metadata = NYamlConfig::GetMainMetadata(str);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Version, 10);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Cluster, "foo");
        }

        {
            TString str = R"(
metadata:
  version: 10
)";
            auto metadata = NYamlConfig::GetMainMetadata(str);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Version, 10);
            UNIT_ASSERT(!metadata.Cluster);
        }

        {
            TString str = R"(
metadata:
  cluster: foo
)";
            auto metadata = NYamlConfig::GetMainMetadata(str);
            UNIT_ASSERT(!metadata.Version);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Cluster, "foo");
        }

        {
            TString str = R"(
metadata: {}
)";
            auto metadata = NYamlConfig::GetMainMetadata(str);
            UNIT_ASSERT(!metadata.Version);
            UNIT_ASSERT(!metadata.Cluster);
        }

        {
            TString str = "foo: bar";
            auto metadata = NYamlConfig::GetMainMetadata(str);
            UNIT_ASSERT(!metadata.Version);
            UNIT_ASSERT(!metadata.Cluster);
        }
    }

    Y_UNIT_TEST(ReplaceMetadata) {
        NYamlConfig::TMainMetadata metadata;
        metadata.Version = 1;
        metadata.Cluster = "test";

        {
            TString str = R"(
# comment1
{value: 1, array: [{test: "1"}], obj: {value: 2}} # comment2
# comment3
)";

            TString exp = R"(metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
value: 1
array:
- test: "1"
obj:
  value: 2
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(
# comment1
{value: 1, metadata: {version: 1, cluster: "test"}, array: [{test: "1"}], obj: {value: 2}} # comment2
# comment3
)";

            TString exp = R"(metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
value: 1
array:
- test: "1"
obj:
  value: 2
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(metadata: {version: 0, cluster: tes}
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(metadata:
  version: 0
  cluster: tes
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(metadata:
  version: 0
  cool: {foo: bar}
  cluster: tes
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(

---
metadata:
  cluster: tes
  version: 0
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(

---
metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(
---
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(
---
metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        {
            TString str = R"(
---
metadata:
  kind: MainConfig
  version: 0
  cluster: "test"
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(
---
metadata:
  kind: MainConfig
  cluster: "test"
  version: 1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }

        metadata.Cluster = "";

        {
            TString str = R"(
---
metadata:
  version: 1
  cluster:
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString exp = R"(
---
metadata:
  kind: MainConfig
  cluster: ""
  version: 1
# comment1
value: 1
array: [{test: "1"}]
obj: {value: 2} # comment2
# comment3
)";

            TString res = NYamlConfig::ReplaceMetadata(str, metadata);

            UNIT_ASSERT_VALUES_EQUAL(res, exp);
        }
    }

Y_UNIT_TEST(FuseConfigs_ConsoleWins) {
    // Console has key, base has same key -> console wins
    const char* base = R"(
actor_system_config:
  threads: 4
log_config:
  level: INFO
)";
    const char* console = R"(
metadata:
  kind: MainConfig
  version: 1
config:
  actor_system_config:
    threads: 8
allowed_labels:
  tenant:
    type: string
selector_config: []
)";
    auto result = NYamlConfig::FuseConfigs(base, console);
    auto root = result.Root().Map();

    // Console value preserved
    UNIT_ASSERT_VALUES_EQUAL(root.at("config").Map().at("actor_system_config").Map().at("threads").Scalar(), "8");
    // Base value added (missing in console)
    UNIT_ASSERT(root.at("config").Map().Has("log_config"));
    // Metadata from console
    UNIT_ASSERT(root.Has("metadata"));
    // Selectors preserved
    UNIT_ASSERT(root.Has("selector_config"));
    // Allowed labels preserved
    UNIT_ASSERT(root.Has("allowed_labels"));
}

Y_UNIT_TEST(FuseConfigs_BaseFillsGaps) {
    // Base has key not in console -> base value added
    const char* base = R"(
log_config:
  level: DEBUG
feature_flags:
  enable_x: true
)";
    const char* console = R"(
metadata:
  kind: MainConfig
config:
  log_config:
    level: INFO
)";
    auto result = NYamlConfig::FuseConfigs(base, console);
    auto configMap = result.Root().Map().at("config").Map();

    // Console value wins for existing key
    UNIT_ASSERT_VALUES_EQUAL(configMap.at("log_config").Map().at("level").Scalar(), "INFO");
    // Base fills gap for missing key
    UNIT_ASSERT(configMap.Has("feature_flags"));
    UNIT_ASSERT_VALUES_EQUAL(configMap.at("feature_flags").Map().at("enable_x").Scalar(), "true");
}

Y_UNIT_TEST(FuseConfigs_EmptyBase) {
    // Empty base config
    const char* base = R"()";
    const char* console = R"(
metadata:
  kind: MainConfig
config:
  log_config:
    level: INFO
)";
    auto result = NYamlConfig::FuseConfigs(base, console);
    auto configMap = result.Root().Map().at("config").Map();

    // Console config preserved
    UNIT_ASSERT(configMap.Has("log_config"));
    UNIT_ASSERT_VALUES_EQUAL(configMap.at("log_config").Map().at("level").Scalar(), "INFO");
}

Y_UNIT_TEST(FuseConfigs_EmptyConsoleConfig) {
    // Console has empty config section
    const char* base = R"(
log_config:
  level: DEBUG
)";
    const char* console = R"(
metadata:
  kind: MainConfig
config: {}
)";
    auto result = NYamlConfig::FuseConfigs(base, console);
    auto configMap = result.Root().Map().at("config").Map();

    // Base fills all gaps
    UNIT_ASSERT(configMap.Has("log_config"));
    UNIT_ASSERT_VALUES_EQUAL(configMap.at("log_config").Map().at("level").Scalar(), "DEBUG");
}

Y_UNIT_TEST(FuseConfigs_ExcludesStorageOnlyKeys) {
    // Storage-only keys from base should be excluded
    const char* base = R"(
log_config:
  level: DEBUG
hosts:
  - host: node1
    port: 19001
host_configs:
  - host_config_id: 1
blob_storage_config:
  service_set: {}
nameservice_config:
  node:
    - node_id: 1
static_erasure: mirror-3-dc
feature_flags:
  enable_x: true
)";
    const char* console = R"(
metadata:
  kind: MainConfig
config:
  log_config:
    level: INFO
)";
    auto result = NYamlConfig::FuseConfigs(base, console);
    auto configMap = result.Root().Map().at("config").Map();

    // Console value wins
    UNIT_ASSERT_VALUES_EQUAL(configMap.at("log_config").Map().at("level").Scalar(), "INFO");
    // Non-storage key from base is included
    UNIT_ASSERT(configMap.Has("feature_flags"));
    // Storage-only keys are excluded
    UNIT_ASSERT(!configMap.Has("hosts"));
    UNIT_ASSERT(!configMap.Has("host_configs"));
    UNIT_ASSERT(!configMap.Has("blob_storage_config"));
    UNIT_ASSERT(!configMap.Has("nameservice_config"));
    UNIT_ASSERT(!configMap.Has("static_erasure"));
}
}

Y_UNIT_TEST_SUITE(YamlConfigResolveUnique) {

Y_UNIT_TEST(NotUniqueSelectors) {
    const char* configWithNotUniqueSelectors = R"(---
allowed_labels:
  label1:
    type: enum
    values:
      ? a
      ? b
  label2:
    type: enum
    values:
      ? x
      ? y
      ? z
  label3:
    type: enum
    values:
      ? p
      ? q
      ? r
config:
  value: base
selector_config:
- description: "selector1"
  selector:
    label1: a
  config:
    value: config_a
- description: "selector2"
  selector:
    label1: b
  config:
    value: config_b
- description: "selector3"
  selector:
    label2: x
  config: {}
- description: "selector4"
  selector:
    label3: p
  config: {}
)";

    auto docAll = NFyaml::TDocument::Parse(configWithNotUniqueSelectors);
    auto resolvedAll = NYamlConfig::ResolveAll(docAll);

    auto docUniq = NFyaml::TDocument::Parse(configWithNotUniqueSelectors);
    TVector<NYamlConfig::TDocumentConfig> resolvedUniq;
    NYamlConfig::ResolveUniqueDocs(
        docUniq,
        [&](NYamlConfig::TDocumentConfig&& cfg) {
            resolvedUniq.push_back(std::move(cfg));
        });

    UNIT_ASSERT(resolvedUniq.size() < resolvedAll.Configs.size());
}

Y_UNIT_TEST(AllTestConfigs) {
    auto testConfig = [](const char* config, const char* name) {
        auto docAll = NFyaml::TDocument::Parse(config);
        auto resolvedAll = NYamlConfig::ResolveAll(docAll);

        auto docUniq = NFyaml::TDocument::Parse(config);
        TVector<NYamlConfig::TDocumentConfig> resolvedUniq;
        NYamlConfig::ResolveUniqueDocs(
            docUniq,
            [&](NYamlConfig::TDocumentConfig&& cfg) {
                resolvedUniq.push_back(std::move(cfg));
            });

        auto toStr = [](const NFyaml::TNodeRef& node) {
            TStringStream ss;
            ss << node;
            return ss.Str();
        };

        TSet<TString> allDocs;
        for (auto& [_, cfg] : resolvedAll.Configs) {
            auto doc = cfg.first.Clone();
            for (auto it = doc.begin(); it != doc.end(); ++it) {
                it->RemoveTag();
            }
            auto cleanConfig = doc.Root().Map().at("config");
            allDocs.insert(toStr(cleanConfig));
        }

        TSet<TString> uniqDocs;
        for (auto& cfg : resolvedUniq) {
            uniqDocs.insert(toStr(cfg.second));
        }

        UNIT_ASSERT_VALUES_EQUAL_C(uniqDocs.size(), resolvedUniq.size(),
            TString("Config: ") + name + ", ResolveUniqueDocs has duplicates");

        for (const auto& s : uniqDocs) {
            UNIT_ASSERT_C(allDocs.contains(s),
                TString("Config: ") + name + ", ResolveUniqueDocs has extra doc not in ResolveAll");
        }

        UNIT_ASSERT_VALUES_EQUAL_C(allDocs.size(), uniqDocs.size(),
            TString("Config: ") + name + ", size mismatch");

        for (const auto& s : allDocs) {
            UNIT_ASSERT_C(uniqDocs.contains(s),
                TString("Config: ") + name + ", ResolveUniqueDocs missing doc from ResolveAll");
        }
    };

    testConfig(WholeConfig, "WholeConfig");
    testConfig(UnresolvedAllConfig, "UnresolvedAllConfig");
    testConfig(UnresolvedSimpleConfig1, "UnresolvedSimpleConfig1");
    testConfig(UnresolvedSimpleConfig2, "UnresolvedSimpleConfig2");
    testConfig(UnresolvedSimpleConfig3, "UnresolvedSimpleConfig3");
    testConfig(UnresolvedSimpleConfigAppend, "UnresolvedSimpleConfigAppend");
}

}

Y_UNIT_TEST_SUITE(ApplySelectors) {
/**
 * Tests:
 *  1. None                   - Successfull no-change resolving with no config
 *  2. Empty                  - Successfull no-change resolving with empty config
 *  3. NoSelectors            - Successfull no-change resolving with no selectors
 *  4. AddToMap               - Selector-based parameters addition to map
 *  5. AddToList              - Selector-based elements addition to list
 *  6. ModifyInMap            - Selector-based parameters modification in map
 *  7. ModifyInList           - Selector-based parameters modification in list
 *  8. ModifyAbsentInMap      - Edge cases of absent map elements modification with selectors
 *  9. ModifyAbsentInList     - Edge cases of absent list elements modification with selectors.
 *                              PAY ATTENTION: has erroneous behavior with '!remove' tag
 * 10. LabeledSelectors       - Label-respected selector applying
 * 11. MultipleSelectors      - Multiple selectors layering
 *
 * Notes:
 *  - tests 4-9 performed for all possible parameter types (scalar, list, map)
 *    with with tags and nested levels
 */

     Y_UNIT_TEST(None)
    {
        const TString config = R"(
fictive:
  a: 1
  b:
    c: 2
)";

        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 2; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, labels);

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(config));

            labels.emplace("some_label", "a");
        }
    }

     Y_UNIT_TEST(NoneWithSelectors)
    {
        const TString config = R"(
fictive:
  a: 1
  b:
    c: 2
selector_config:
- description: some none
  selector: {}
  config:
    param: 1
- description: some a
  selector:
    some_label: a
  config:
    param: 2
)";

        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 2; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, labels);

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(config));

            labels.emplace("some_label", "a");
        }
    }

     Y_UNIT_TEST(Empty)
    {
        const TString config = R"(
config:
)";
        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 2; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, labels);

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(config));

            labels.emplace("some_label", "a");
        }
    }

    Y_UNIT_TEST(NoSelectors)
    {
        const TString config = R"(
config:
  subconfig_1:
    param_1: none
  subconfig_2: !inherit
    param_2: true
)";
        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 2; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, labels);

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(config));

            labels.emplace("some_label", "a");
        }
    }

    Y_UNIT_TEST(AddToMap)
    {
        const TString config = R"(
config:
  subconfig:
    field_1: f1
    map_1:
      field_1: m1f1
      map_1:
        field_1: m1m1f1
      list_1:
      - name: a
        field_1: m1l1af1
      - name: b
        field_1: m1l1bf1
selector_config:
- description: add to map
  selector: {}
  config:
    subconfig: !inherit
      field_2: f2
      map_1: !inherit
        field_2: m1f2
        map_1: !inherit
          field_2: m1m1f2
        map_2:
          field_1: m1m2f1
        list_2:
        - name: c
          field_1: m1l2cf1
        - name: d
          field_1: m1l2df1
      list_1:
      - name: e
        field_1: l1ef1
)";
        const TString expected = R"(
subconfig:
  field_1: f1
  map_1:
    field_1: m1f1
    map_1:
      field_1: m1m1f1
      field_2: m1m1f2
    list_1:
    - name: a
      field_1: m1l1af1
    - name: b
      field_1: m1l1bf1
    field_2: m1f2
    map_2:
      field_1: m1m2f1
    list_2:
    - name: c
      field_1: m1l2cf1
    - name: d
      field_1: m1l2df1
  field_2: f2
  list_1:
  - name: e
    field_1: l1ef1
)";

        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ApplySelectors(doc, {});

        TStringStream resolved;
        resolved << doc.Root().Map().at("config");
        UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
    }

    Y_UNIT_TEST(AddToList)
    {
        const TString config = R"(
config:
  subconfig:
    list_1:
    - name: a
      field_1: l1af1
    - name: b
      field_1: l1bf1
    list_2:
    - name: a
      field_1: l2af1
    - name: b
      field_1: l2bf1
selector_config:
- description: add to list
  selector: {}
  config:
    subconfig: !inherit
      list_1: !inherit:name   # existing elements shall be fully replaced
      - name: a
        field_1: l1af1-new
      - name: c
        field_1: l1cf1
      list_2: !append         # existing elements shall be kept, duplicates shall be added
      - name: a
        field_1: l2af1-new
      - name: c
        field_1: l2cf1
)";
        const TString expected = R"(
subconfig:
  list_1:
  - name: a
    field_1: l1af1-new
  - name: b
    field_1: l1bf1
  - name: c
    field_1: l1cf1
  list_2:
  - name: a
    field_1: l2af1
  - name: b
    field_1: l2bf1
  - name: a
    field_1: l2af1-new
  - name: c
    field_1: l2cf1
)";
        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ApplySelectors(doc, {});

        TStringStream resolved;
        resolved << doc.Root().Map().at("config");
        UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
    }

    Y_UNIT_TEST(ModifyInMap)
    {
        const TString config = R"(
config:
  subconfig_1:

    field_1: f1
    field_2: f2-unchanged

    map_1:
      field_1: m1f1
      field_2: m1f2-unchanged
      map_1:
        field_1: m1m1f1
        field_2: m1m1f2-unchanged
      map_2:
        field_1: m1m2f1
        field_2: m1m2f2
      list_1:
      - name: a
        field_1: m1l1af1
      - name: b
        field_1: m1l1bf1

    map_2-unchanged:
      field_1: m2f1-unchanged

    list_1:
    - name: a
      field_1: l1af1

  subconfig_2:
    field_1: f1
    field_2: f1
    map_1:
      field_1: m1f1
    list_1:
    - name: a
      field_1: l1af1

  subconfig_3:
    field_1: f1
    field_2: f1
    map_1:
      field_1: m1f1
    list_1:
    - name: a
      field_1: l1af1

  subconfig_4:
    field_1: f1-unchanged
    field_2: f1-unchanged

selector_config:
- description:
  selector: {}
  config:

    subconfig_1: !inherit       # map:   shall be appended/modified

      field_1: f1-new           # field: shall be replaced
      field_3: f3-new           # field: shall be added

      map_1: !inherit           # map:   shall be appended/modified

        field_1: m1f1-new       # field: shall be replaced
        field_3: m1f3-new       # field: shall be added

        map_1: !inherit         # map:   shall be appended/modified
          field_1: m1m1f1-new   # field: shall be replaced
          field_3: m1m1f3-new   # field: shall be added

        map_2:                  # map:   shall be replaced
          field_3: m1m2f3-new

        map_3_1:                # map:   shall be added
          field_1: m1m31f1-new

        list_1:                 # list:  shall be replaced
        - name: c
          field_1: m1l1cf1-new

        list_2_1:               # list:  shall be added
        - name: a
          field_1: m1l21af1-new

      list_1:                   # list: shall be replaced
      - name: d
        field_1: l1df1-new

      list_2_1:                 # list:  shall be added
      - name: a
        field_1: l21af1-new

    subconfig_2:                # map:  shall be replaced
      field_1: f1-new
      field_3: f3-new

    subconfig_3:                # map:  shall be replaced

    subconfig_5_1:              # map:  shall be added as not existing

    subconfig_5_2:              # map:  shall be added as not existing
      field_1: f1-new
      field_3: f3-new
)";
        const TString expected = R"(
subconfig_1:
  field_2: f2-unchanged
  map_1:
    field_2: m1f2-unchanged
    map_1:
      field_2: m1m1f2-unchanged
      field_1: m1m1f1-new
      field_3: m1m1f3-new
    field_1: m1f1-new
    field_3: m1f3-new
    map_2:
      field_3: m1m2f3-new
    map_3_1:
      field_1: m1m31f1-new
    list_1:
    - name: c
      field_1: m1l1cf1-new
    list_2_1:
    - name: a
      field_1: m1l21af1-new
  map_2-unchanged:
    field_1: m2f1-unchanged
  field_1: f1-new
  field_3: f3-new
  list_1:
  - name: d
    field_1: l1df1-new
  list_2_1:
  - name: a
    field_1: l21af1-new
subconfig_4:
  field_1: f1-unchanged
  field_2: f1-unchanged
subconfig_2:
  field_1: f1-new
  field_3: f3-new
subconfig_3: )" R"(
subconfig_5_1: )" R"(
subconfig_5_2:
  field_1: f1-new
  field_3: f3-new
)";
        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ApplySelectors(doc, {});

        TStringStream resolved;
        resolved << doc.Root().Map().at("config");
        UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
    }

    Y_UNIT_TEST(ModifyInList)
    {
        const TString config = R"(
config:
  subconfig_1:

    list_1:
    - name: a
      field_1: l1af1
      map_1:
        field_1: l1am1f1
    - name: b
      field_1: l1bf1
      map_1:
        field_1: l1bm1f1
    - name: c
      field_1: l1cf1-unchanged
      map_1:
        field_1: l1cm1f1-unchanged

    list_2:
    - name: a
      field_1: l2af1
      map_1:
        field_1: l2am1f1
    - name: b
      field_1: l2bf1
    - name: c
      field_1: l2cf1

selector_config:
- description:
  selector: {}
  config:
    subconfig_1: !inherit

      list_1: !inherit:name       # shall remove/replace existing elements (by key 'name') and append new
      - name: a                   # shall be replaced
        field_2: l1af2-new
        map_1:
          field_2: l1am1f2-new
        map_2:
          field_1: l1am2f1-new
      - !remove                   # shall be removed ('name: b')
        name: b
      - name: d                   # shall be added
        field_3: l1df3-new
        map_3:
          field_1: l1dm3f1-new

      list_2: !append             # shall unconditionally append elements (even duplicates)
      - name: b
        field_1: l2bf1
        map_1:
          field_1: l2bm1f1
      - name: d
        field_1: l2df1
        map_1:
          field_1: l2dm1f1
      - name: e
        field_1: l2ef1
        map_1:
          field_1: l2em1f1
      - name: a
        field_1: l2af1
        map_1:
          field_1: l2am1f1
)";
        const TString expected = R"(
subconfig_1:
  list_1:
  - name: a
    field_2: l1af2-new
    map_1:
      field_2: l1am1f2-new
    map_2:
      field_1: l1am2f1-new
  - name: c
    field_1: l1cf1-unchanged
    map_1:
      field_1: l1cm1f1-unchanged
  - name: d
    field_3: l1df3-new
    map_3:
      field_1: l1dm3f1-new
  list_2:
  - name: a
    field_1: l2af1
    map_1:
      field_1: l2am1f1
  - name: b
    field_1: l2bf1
  - name: c
    field_1: l2cf1
  - name: b
    field_1: l2bf1
    map_1:
      field_1: l2bm1f1
  - name: d
    field_1: l2df1
    map_1:
      field_1: l2dm1f1
  - name: e
    field_1: l2ef1
    map_1:
      field_1: l2em1f1
  - name: a
    field_1: l2af1
    map_1:
      field_1: l2am1f1
)";
        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ApplySelectors(doc, {});

        TStringStream resolved;
        resolved << doc.Root().Map().at("config");
        UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
    }

    Y_UNIT_TEST(ModifyAbsentInMap)
    {
        const TString config = R"(
config:
  subconfig_1:
    map_1:
      field_1: m1f1

selector_config:
- description:
  selector: {}
  config:

    subconfig_1: !inherit

      map_1: !inherit

        map_1: !inherit         # map:   shall be added keeping '!inherit' tag as not existing
          field_1: m1m1f1-new

        list_1: !inherit:name   # list:  shall be added "as is" with tag '!inherit:name' as not existing
        - name: a
          field_1: m1m1l1af1-new

      list_1: !inherit:name     # list:  shall be added "as is" with tag '!inherit:name' as not existing
      - name: a
        field_1: l1af1-new

    subconfig_2: !inherit       # map:  shall be added "as is" with tag '!inherit' as not existing

    subconfig_3: !inherit       # map:  shall be added "as is" with tag '!inherit' as not existing
      field_1: f1-new
      map_1: !inherit           # map:  shall be added "as is" with tag '!inherit' as not existing
        field_1: m1f1-new
)";
        const TString expected = R"(
subconfig_1:
  map_1:
    field_1: m1f1
    map_1: !inherit
      field_1: m1m1f1-new
    list_1: !inherit:name
    - name: a
      field_1: m1m1l1af1-new
  list_1: !inherit:name
  - name: a
    field_1: l1af1-new
subconfig_2: !inherit )" R"(
subconfig_3: !inherit
  field_1: f1-new
  map_1: !inherit
    field_1: m1f1-new
)";
        // All migrated tags must be removed with NYamlConfig::RemoveTags()
        const TString expectedAfterRemoveTags = R"(
subconfig_1:
  map_1:
    field_1: m1f1
    map_1:
      field_1: m1m1f1-new
    list_1:
    - name: a
      field_1: m1m1l1af1-new
  list_1:
  - name: a
    field_1: l1af1-new
subconfig_2: )" R"(
subconfig_3:
  field_1: f1-new
  map_1:
    field_1: m1f1-new
)";

        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ApplySelectors(doc, {});

        {
            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
        }

        {
            NYamlConfig::RemoveTags(doc);

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedAfterRemoveTags));
        }
    }

    Y_UNIT_TEST(ModifyAbsentInList)
    {
        const TString config = R"(
config:
  subconfig_1:

    list_1:
    - name: a
      field_1: l1af1
      map_1:
        field_1: l1am1f1

    list_2:
    - name: a
      field_1: l2af1
      map_1:
        field_1: l2am1f1

selector_config:
- description:
  selector: {}
  config:
    subconfig_1: !inherit

      list_1: !inherit:name       # shall remove/replace existing elements (by key 'name') and append new
      - name: a                   # shall be replaced
        field_1: l1af1-new
        map_1: !inherit           # shall be added "as is" with tag !inherit as not existing (whole element is replaced)
          field_1: l1am1f1-new
      - name: b                   # shall be added
        field_1: l1bf1-new
        map_1: !inherit           # shall be added "as is" with tag !inherit as not existing (whole element is replaced)
          field_1: l1bm1f1-new
      - !remove                   # shall be added "as is" with tag !remove as not existing
        name: c

      list_2: !append             # shall unconditionally append elements (even duplicates)
      - name: a                   # shall be added
        field_1: l2af1-new
        map_1: !inherit           # shall be added "as is" with tag !inherit as not existing (whole element is replaced)
          field_1: l2am1f1-new
      - name: b                   # shall be added
        field_1: l2bf1-new
        map_1: !inherit           # shall be added "as is" with tag !inherit as not existing (whole element is replaced)
          field_1: l2bm1f1-new
      - !remove                   # shall be added "as is" with tag !remove as not existing
        name: c

      list_3: !append             # shall be added "as is" with tag !inherit:name as not existing
      - name: a
        field_1: l3af1-new
        map_1: !inherit           # shall be added "as is" with tag !inherit as not existing
          field_1: l3am1f1-new
      - !remove                   # shall be added "as is" with tag !remove as not existing
        name: c

      list_4: !inherit:name       # shall be added "as is" with tag !inherit:name as not existing
      - name: a
        field_1: l4af1-new
        map_1: !inherit           # shall be added "as is" with tag !inherit as not existing
          field_1: l4am1f1-new
      - !remove                   # shall be added "as is" with tag !remove as not existing
        name: c
)";
        const TString expected = R"(
subconfig_1:
  list_1:
  - name: a
    field_1: l1af1-new
    map_1: !inherit
      field_1: l1am1f1-new
  - name: b
    field_1: l1bf1-new
    map_1: !inherit
      field_1: l1bm1f1-new
  - !remove
    name: c
  list_2:
  - name: a
    field_1: l2af1
    map_1:
      field_1: l2am1f1
  - name: a
    field_1: l2af1-new
    map_1: !inherit
      field_1: l2am1f1-new
  - name: b
    field_1: l2bf1-new
    map_1: !inherit
      field_1: l2bm1f1-new
  - !remove
    name: c
  list_3: !append
  - name: a
    field_1: l3af1-new
    map_1: !inherit
      field_1: l3am1f1-new
  - !remove
    name: c
  list_4: !inherit:name
  - name: a
    field_1: l4af1-new
    map_1: !inherit
      field_1: l4am1f1-new
  - !remove
    name: c
)";
        // All migrated tags must be removed with NYamlConfig::RemoveTags(),
        // except '!remove' erroneous behavior: 'name:c' shall be added instead do nothing
        const TString expectedAfterRemoveTags = R"(
subconfig_1:
  list_1:
  - name: a
    field_1: l1af1-new
    map_1:
      field_1: l1am1f1-new
  - name: b
    field_1: l1bf1-new
    map_1:
      field_1: l1bm1f1-new
  - name: c)" /* ERROR: 'name:c' has been added instead do nothing */ R"(
  list_2:
  - name: a
    field_1: l2af1
    map_1:
      field_1: l2am1f1
  - name: a
    field_1: l2af1-new
    map_1:
      field_1: l2am1f1-new
  - name: b
    field_1: l2bf1-new
    map_1:
      field_1: l2bm1f1-new
  - name: c)" /* ERROR: 'name:c' has been added instead do nothing */ R"(
  list_3:
  - name: a
    field_1: l3af1-new
    map_1:
      field_1: l3am1f1-new
  - name: c)" /* ERROR: 'name:c' has been added instead do nothing */ R"(
  list_4:
  - name: a
    field_1: l4af1-new
    map_1:
      field_1: l4am1f1-new
  - name: c)" /* ERROR: 'name:c' has been added instead do nothing */ R"(
)";
        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ApplySelectors(doc, {});

        {
            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
        }

        {
            NYamlConfig::RemoveTags(doc);

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedAfterRemoveTags));
        }
    }

    Y_UNIT_TEST(LabeledSelectors)
    {
        const TString config = R"(
config:
  subconfig_1: !inherit
    field_1: 1
    field_2: 2
    map_1: !inherit
      field_1: 1
      field_2: 2
      list_1: !inherit:name
      - name: a
        field_1: 1
        field_2: 2
      - name: x
        field_1: 1
      - name: y
        field_1: 1
    list_1: !append
    - name: a
      field_1: 1
      field_2: 2

selector_config:

- description:
  selector:
    type: x
  config:
    subconfig_1: !inherit
      field_1: 1x
      map_1: !inherit
        field_1: 1x
        list_1: !inherit:name
        - name: a
          field_1: 1x
        - !remove
          name: y
      list_1: !append
      - name: a
        field_1: 1x

- description:
  selector:
    type: y
  config:
    subconfig_1: !inherit
      field_1: 1y
      map_1: !inherit
        field_1: 1y
        list_1: !inherit:name
        - name: a
          field_1: 1y
        - !remove
          name: x
      list_1: !append
      - name: a
        field_1: 1y

- description:
  selector:
    type:
      in:
      - q
      - w
  config:
    subconfig_1: !inherit
      field_1: 1qw
      map_1: !inherit
        field_1: 1qw
        list_1: !inherit:name
        - name: a
          field_1: 1qw
        - !remove
          name: x
        - !remove
          name: y
      list_1: !append
      - name: a
        field_1: 1qw
)";
        const TString expectedNone = R"(
subconfig_1: !inherit
  field_1: 1
  field_2: 2
  map_1: !inherit
    field_1: 1
    field_2: 2
    list_1: !inherit:name
    - name: a
      field_1: 1
      field_2: 2
    - name: x
      field_1: 1
    - name: y
      field_1: 1
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
)";
        const TString expectedX = R"(
subconfig_1: !inherit
  field_2: 2
  map_1: !inherit
    field_2: 2
    list_1: !inherit:name
    - name: a
      field_1: 1x
    - name: x
      field_1: 1
    field_1: 1x
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: a
    field_1: 1x
  field_1: 1x
)";
        const TString expectedY = R"(
subconfig_1: !inherit
  field_2: 2
  map_1: !inherit
    field_2: 2
    list_1: !inherit:name
    - name: a
      field_1: 1y
    - name: y
      field_1: 1
    field_1: 1y
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: a
    field_1: 1y
  field_1: 1y
)";
        const TString expectedQW = R"(
subconfig_1: !inherit
  field_2: 2
  map_1: !inherit
    field_2: 2
    list_1: !inherit:name
    - name: a
      field_1: 1qw
    field_1: 1qw
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: a
    field_1: 1qw
  field_1: 1qw
)";
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedNone));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type", "x"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedX));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type", "y"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedY));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type", "q"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedQW));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type", "w"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedQW));
        }
    }

    Y_UNIT_TEST(MultipleSelectors)
    {
        const TString config = R"(
config:
  subconfig_1: !inherit
    field_1: 1
    field_2: 2
    map_1: !inherit
      field_1: 1
      field_2: 2
      list_1: !inherit:name
      - name: a
        field_1: 1
        field_2: 2
      - name: _
        field_1: 1
      - name: x
        field_1: 1
      - name: y
        field_1: 1
    list_1: !append
    - name: a
      field_1: 1
      field_2: 2

selector_config:

- description:  all
  selector: {}
  config:
    subconfig_1: !inherit
      field_2: 3
      list_1: !append
      - name: _
        field_2: 3

- description:  all
  selector: {}
  config:
    subconfig_1: !inherit
      map_1: !inherit
        field_2: 4
        list_1: !inherit:name
        - name: a
          field_2: 4
        - !remove
          name: _

- description:  type_1 = x && type_2 = ???
  selector:
    type_1: x
  config:
    subconfig_1: !inherit
      field_1: 1x
      list_1: !append
      - name: a
        field_1: 1x

- description:  type_1 = x && type_2 = ???
  selector:
    type_1: x
  config:
    subconfig_1: !inherit
      map_1: !inherit
        field_1: 1x
        list_1: !inherit:name
        - name: a
          field_1: 1x
        - !remove
          name: y

- description:  type_1 = ??? && type_2 = y
  selector:
    type_2: y
  config:
    subconfig_1: !inherit
      field_1: 1y
      list_1: !append
      - name: a
        field_1: 1y

- description:  type_1 = ??? && type_2 = y
  selector:
    type_2: y
  config:
    subconfig_1: !inherit
      map_1: !inherit
        field_1: 1y
        list_1: !inherit:name
        - name: a
          field_1: 1y
        - !remove
          name: x

- description:  type_1 = x && type_2 = y
  selector:
    type_1: x
    type_2: y
  config:
    subconfig_1: !inherit
      field_2: 2xy
      list_1: !append
      - name: a
        field_2: 2xy

- description:  type_1 = x && type_2 = y
  selector:
    type_1: x
    type_2: y
  config:
    subconfig_1: !inherit
      map_1: !inherit
        field_2: 2xy

- description:  type_1 = x && type_2 != y
  selector:
    type_1:
      in:
      - o
      - x
    type_2:
      not_in:
      - y
      - o
  config:
    subconfig_1: !inherit
      field_3: 3ox
      list_1: !append
      - name: a
        field_3: 3ox

- description:  type_1 = x && type_2 != y
  selector:
    type_1:
      in:
      - o
      - x
    type_2:
      not_in:
      - y
      - o
  config:
    subconfig_1: !inherit
      map_1: !inherit
        field_3: 3ox
)";
        const TString expectedNone = R"(
subconfig_1: !inherit
  field_1: 1
  map_1: !inherit
    field_1: 1
    list_1: !inherit:name
    - name: a
      field_2: 4
    - name: x
      field_1: 1
    - name: y
      field_1: 1
    field_2: 4
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: _
    field_2: 3
  field_2: 3
)";
        const TString expectedX = R"(
subconfig_1: !inherit
  map_1: !inherit
    list_1: !inherit:name
    - name: a
      field_1: 1x
    - name: x
      field_1: 1
    field_2: 4
    field_1: 1x
    field_3: 3ox
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: _
    field_2: 3
  - name: a
    field_1: 1x
  - name: a
    field_3: 3ox
  field_2: 3
  field_1: 1x
  field_3: 3ox
)";
        const TString expectedY = R"(
subconfig_1: !inherit
  map_1: !inherit
    list_1: !inherit:name
    - name: a
      field_1: 1y
    - name: y
      field_1: 1
    field_2: 4
    field_1: 1y
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: _
    field_2: 3
  - name: a
    field_1: 1y
  field_2: 3
  field_1: 1y
)";
        const TString expectedXY = R"(
subconfig_1: !inherit
  map_1: !inherit
    list_1: !inherit:name
    - name: a
      field_1: 1y
    field_1: 1y
    field_2: 2xy
  list_1: !append
  - name: a
    field_1: 1
    field_2: 2
  - name: _
    field_2: 3
  - name: a
    field_1: 1x
  - name: a
    field_1: 1y
  - name: a
    field_2: 2xy
  field_1: 1y
  field_2: 2xy
)";
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedNone));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type_1", "x"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedX));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type_2", "y"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedY));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ApplySelectors(doc, {
                NYamlConfig::TNamedLabel{"type_1", "x"},
                NYamlConfig::TNamedLabel{"type_2", "y"}});

            TStringStream resolved;
            resolved << doc.Root().Map().at("config");
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedXY));
        }
    }
}

Y_UNIT_TEST_SUITE(YamlDatabaseConfigResolve) {

/**
 * Tests:
 *  1. Empty                  - Successfull no-change resolving with empty config
 *  2. NoSelectors            - Successfull no-change resolving with no selectors
 *  3. DropLabelsAndSelectors - Drop 'allowed_labels' and 'selector_config' sections after resolving
 *  4. PreserveExistingTags   - Preserve existing elements tags at all levels
 *  5. SingleSelector         - Single selector applying
 *  6. MultipleSelectors      - Multiple selectors layering
 *
 * Notes:
 *  - tests 1-4 performed both without and with labels passed into ResolveDatabaseConfig() function
 */

     Y_UNIT_TEST(Empty)
    {
        const TString config = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
)";
        const TString expected = config;

        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 2; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, labels);

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));

            labels.emplace("type", "a");
        }
    }

    Y_UNIT_TEST(NoSelectors)
    {
        const TString config = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1:
    param_1: none
  subconfig_2: !inherit
    param_2: true
)";
        const TString expected = config;

        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 2; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, labels);

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));

            labels.emplace("type", "a");
        }
    }

    Y_UNIT_TEST(ResolveDropLabelsAndSelectors)
    {
        const TString config = R"(---
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0

config:
  subconfig_1:
    field_1: none
  subconfig_2: !inherit
    field_2: true

allowed_labels:
  type_1:
    type: string
  type_2:
    type: enum
    values:
      ? a
      ? b

selector_config:

- description: all
  selector: {}
  config:
    subconfig_1: !inherit
      field_1: all

- description:  type = a
  selector:
    type_1: a
  config:
    subconfig_1: !inherit
      field_1: a
)";
        // Without and with labels
        TSet<NYamlConfig::TNamedLabel> labels;
        for (int i = 0; i < 3; i++)
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {});

            UNIT_ASSERT(!doc.Root().Map().Has("allowed_labels"));
            UNIT_ASSERT(!doc.Root().Map().Has("selector_config"));

            if (!i) {
                labels.emplace("type_1", "x");
            }
            else {
                labels.emplace("type_1", "a");
            }
        }
    }

    Y_UNIT_TEST(PreserveExistingTags)
    {
        const TString config = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0

config:

  subconfig_1: !inherit
    map_1: !inherit
      field_1: 1
      list_1: !inherit:name
      - name: a
        map_1: !inherit
          field_1: 1
      - !remove
        name: b

    list_1: !inherit:name
    - name: a
      map_1: !inherit
        field_1: 1
    - !remove
      name: b

    list_2: !append
    - name: a
      map_1:
        field_1: 1
    - !remove
      name: b

selector_config:

- description:
  selector: {}
  config:

    subconfig_1: !inherit
      map_1: !inherit
        field_1: 2
        list_1: !append
        - name: c
          map_1:
            field_1: 2

      list_1: !inherit:name
      - name: a
        map_1:
          field_1: 2

      list_2: !append
      - name: c
        map_1:
          field_1: 2

      # all tags of new element shall not be preserved
      # (except errorneous '!remove', see Y_UNIT_TEST(ModifyAbsentInList))

      list_3: !append
      - name: a
        map_1: !inherit
          field_1: 3
        list_1: !inherit:name
        - name: a
          field_1: 3
      map_3: !inherit
        map_1: !inherit
          field_1: 3
        list_1: !append
        - name: c
          map_1: !inherit
            field_1: 3
)";
        const TString expected = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    map_1: !inherit
      list_1: !inherit:name
      - name: a
        map_1: !inherit
          field_1: 1
      - !remove
        name: b
      - name: c
        map_1:
          field_1: 2
      field_1: 2
    list_1: !inherit:name
    - name: a
      map_1: !inherit
        field_1: 2
    - !remove
      name: b
    list_2: !append
    - name: a
      map_1:
        field_1: 1
    - !remove
      name: b
    - name: c
      map_1:
        field_1: 2
    list_3:
    - name: a
      map_1:
        field_1: 3
      list_1:
      - name: a
        field_1: 3
    map_3:
      map_1:
        field_1: 3
      list_1:
      - name: c
        map_1:
          field_1: 3
)";
        auto doc = NFyaml::TDocument::Parse(config);
        NYamlConfig::ResolveDatabaseConfig(doc, {});

        TStringStream resolved;
        resolved << doc.Root();
        UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expected));
    }

    Y_UNIT_TEST(SingleSelector)
    {
        const TString config = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0

config:

  subconfig_1: !inherit
    field_0: 0
    field_1: 1

selector_config:

- description:
  selector:
    type: x
  config:
    subconfig_1: !inherit
      field_1: 1x
      field_2: 2x

- description:
  selector:
    type: y
  config:
    subconfig_1: !inherit
      field_1: 1y
      field_2: 2y

- description:
  selector:
    type:
      in:
      - q
      - w
  config:
    subconfig_1: !inherit
      field_1: 1qw
      field_2: 2qw
)";
        const TString expectedNone = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_1: 1
)";
        const TString expectedX = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_1: 1x
    field_2: 2x
)";
        const TString expectedY = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_1: 1y
    field_2: 2y
)";
        const TString expectedQW = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_1: 1qw
    field_2: 2qw
)";
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedNone));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type", "x"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedX));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type", "y"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedY));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type", "q"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedQW));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type", "w"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedQW));
        }
    }

    Y_UNIT_TEST(MultipleSelectors)
    {
        const TString config = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0

config:

  subconfig_1: !inherit
    field_0: 0
    field_1: 1
    field_2: 2

selector_config:

- description:  all
  selector: {}
  config:
    subconfig_1: !inherit
      field_1: _

- description:  all
  selector: {}
  config:
    subconfig_1: !inherit
      field_2: _
      field_3: _
      field_4: _

- description:  type_1 = x && type_2 = ???
  selector:
    type_1: x
  config:
    subconfig_1: !inherit
      field_1: 1x

- description:  type_1 = x && type_2 = ???
  selector:
    type_1: x
  config:
    subconfig_1: !inherit
      field_3: 1x

- description:  type_1 = ??? && type_2 = y
  selector:
    type_2: y
  config:
    subconfig_1: !inherit
      field_2: 1y

- description:  type_1 = ??? && type_2 = y
  selector:
    type_2: y
  config:
    subconfig_1: !inherit
      field_4: 1y

- description:  type_1 = x && type_2 = y
  selector:
    type_1: x
    type_2: y
  config:
    subconfig_1: !inherit
      field_1: 2xy

- description:  type_1 = x && type_2 = y
  selector:
    type_1: x
    type_2: y
  config:
    subconfig_1: !inherit
      field_4: 2xy

- description:  type_1 = x && type_2 != y
  selector:
    type_1:
      in:
      - o
      - x
    type_2:
      not_in:
      - y
      - o
  config:
    subconfig_1: !inherit
      field_2: 3ox

- description:  type_1 = x && type_2 != y
  selector:
    type_1:
      in:
      - o
      - x
    type_2:
      not_in:
      - y
      - o
  config:
    subconfig_1: !inherit
      field_3: 3ox
)";
        const TString expectedNone = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_1: _
    field_2: _
    field_3: _
    field_4: _
)";
        const TString expectedX = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_4: _
    field_1: 1x
    field_2: 3ox
    field_3: 3ox
)";
        const TString expectedY = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_1: _
    field_3: _
    field_2: 1y
    field_4: 1y
)";
        const TString expectedXY = R"(
metadata:
  kind: DatabaseConfig
  database: "/dc/tenant"
  version: 0
config:
  subconfig_1: !inherit
    field_0: 0
    field_3: 1x
    field_2: 1y
    field_1: 2xy
    field_4: 2xy
)";
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedNone));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type_1", "x"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedX));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type_2", "y"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedY));
        }
        {
            auto doc = NFyaml::TDocument::Parse(config);
            NYamlConfig::ResolveDatabaseConfig(doc, {
                NYamlConfig::TNamedLabel{"type_1", "x"},
                NYamlConfig::TNamedLabel{"type_2", "y"}});

            TStringStream resolved;
            resolved << doc.Root();
            UNIT_ASSERT_VALUES_EQUAL(Strip(resolved.Str()), Strip(expectedXY));
        }
    }
}
