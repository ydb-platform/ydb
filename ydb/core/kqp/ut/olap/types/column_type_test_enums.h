#pragma once

namespace NKikimr {
namespace NKqp {

enum EQueryMode {
    SCAN_QUERY,
    EXECUTE_QUERY
};

enum EIntervalTreeMode {
    INTERVAL_TREE_ENABLED,
    INTERVAL_TREE_DISABLED
};

enum ETableKind {
    COLUMNSHARD,
    DATASHARD
};

enum ELoadKind {
    ARROW,
    YDB_VALUE,
    CSV
};

} // namespace NKqp
} // namespace NKikimr
