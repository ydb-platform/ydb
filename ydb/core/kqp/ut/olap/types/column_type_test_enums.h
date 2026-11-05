#pragma once

namespace NKikimr {
namespace NKqp {

enum EQueryMode {
    SCAN_QUERY,
    EXECUTE_QUERY
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

enum class ESerialType {
    SmallSerial,
    Serial,
    BigSerial
};

} // namespace NKqp
} // namespace NKikimr
