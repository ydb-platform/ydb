#pragma once

namespace NKikimr {
namespace NKqp {

enum EQueryMode {
    SCAN_QUERY,
    EXECUTE_QUERY
};

enum ETableKind {
    COLUMN_SHARD,
    DATA_SHARD
};

enum ELoadKind {
    ARROW,
    YDB_VALUE,
    CSV
};

} // namespace NKqp
} // namespace NKikimr
