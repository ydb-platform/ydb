#include "yaml_config.h"

#include <ydb/library/yaml_config/yaml_config_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/key.pb.h>

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

    Y_UNIT_TEST(GetMetadata) {
        {
            TString str = R"(
metadata:
  version: 10
  cluster: foo
)";
            auto metadata = NYamlConfig::GetMetadata(str);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Version, 10);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Cluster, "foo");
        }

        {
            TString str = R"(
metadata:
  version: 10
)";
            auto metadata = NYamlConfig::GetMetadata(str);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Version, 10);
            UNIT_ASSERT(!metadata.Cluster);
        }

        {
            TString str = R"(
metadata:
  cluster: foo
)";
            auto metadata = NYamlConfig::GetMetadata(str);
            UNIT_ASSERT(!metadata.Version);
            UNIT_ASSERT_VALUES_EQUAL(*metadata.Cluster, "foo");
        }

        {
            TString str = R"(
metadata: {}
)";
            auto metadata = NYamlConfig::GetMetadata(str);
            UNIT_ASSERT(!metadata.Version);
            UNIT_ASSERT(!metadata.Cluster);
        }

        {
            TString str = "foo: bar";
            auto metadata = NYamlConfig::GetMetadata(str);
            UNIT_ASSERT(!metadata.Version);
            UNIT_ASSERT(!metadata.Cluster);
        }
    }

    Y_UNIT_TEST(ReplaceMetadata) {
        NYamlConfig::TMetadata metadata;
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
}
