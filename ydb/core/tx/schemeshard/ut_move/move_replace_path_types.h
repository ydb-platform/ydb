#pragma once

// Path object types used to parametrize the move-with-replace tests.
// Serialization is generated via GENERATE_ENUM_SERIALIZATION_WITH_HEADER
// in ya.make, so ToString()/GetEnumAllValues() are available.
enum EMoveReplaceTestPathType {
    RowTable,
    RowTableWithLocalIndex,
    RowTableWithGlobalIndexes,
    RowTableWithLocalAndGlobalIndexes,
    ColumnTable,
    ColumnTableWithIndexes,
    Directory,
    RowTableIndexImplTable,
};
