#pragma once

namespace NKikimr::NKqp::NSetNotNull {

enum class ETableKind {
    Row,
    Column,
};

enum class EFeatureFlag {
    Disabled,
    Enabled,
};

enum class EReboot {
    None,
    SchemeShard,
    TableShard,
    Both,
};

enum class EWriteMode {
    Upsert,
    Insert,
    Replace,
    BulkUpsert,
};

enum class EShardCount {
    One = 1,
    Four = 4,
    Twelve = 12,
};

enum class ENullPosition {
    None,
    First,
    Last,
};

} // namespace NKikimr::NKqp::NSetNotNull
