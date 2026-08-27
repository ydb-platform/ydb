#pragma once

#include <ydb/core/kqp/provider/yql_kikimr_provider.h>
#include <ydb/core/kqp/provider/yql_kikimr_expr_nodes.h>

#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NKqp::NOpt {

struct TBuildWriteInputResult {
    NYql::NNodes::TExprBase Input;
    NYql::NNodes::TCoAtomList Columns;
    // True when the rewrite injected a stream lookup that reads dependency values back
    bool EmittedStreamLookup = false;
};

// Produced by applying each generated column's compiled lambda to a dependency row
struct TGeneratedColumnMembers {
    TVector<NYql::NNodes::TCoAtom> Names;
    TVector<NYql::NNodes::TExprBase> Members;
};

// STORED generated columns of the table, in column order
TVector<const NYql::TKikimrColumnMetadata*> CollectStoredGeneratedColumns(const NYql::TKikimrTableDescription& table);

// Applies each stored generated column's compiled lambda to depRow and produces the matching
// (column name, name-value member) pair
TGeneratedColumnMembers BuildGeneratedColumnMembers(const TVector<const NYql::TKikimrColumnMetadata*>& generatedColumns,
    const NYql::NNodes::TExprBase& depRow, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

// Builds the struct of dependency values fed to a generated column lambda during an UPDATE: each
// dependency is taken from updateStruct when the SET clause provides it, otherwise from rowArg
NYql::NNodes::TExprBase BuildGeneratedDependencyRow(const TVector<const NYql::TKikimrColumnMetadata*>& generatedColumns,
    const NYql::TStructExprType& updateStructType, const NYql::NNodes::TExprBase& updateStruct,
    const NYql::NNodes::TExprBase& rowArg, NYql::TPositionHandle pos, NYql::TExprContext& ctx);

// Appends the STORED generated columns to a write input, computing each value inline or, for a
// partial UPSERT / UPDATE ON that omits a dependency, reading the missing dependencies back from
// the table via a stream lookup
TBuildWriteInputResult ExtendInputRowsWithStoredGeneratedColumns(const NYql::NNodes::TKiWriteTable& write,
    const NYql::NNodes::TExprBase& input, const NYql::NNodes::TCoAtomList& inputColumns,
    const NYql::TKikimrTableDescription& table, NYql::TPositionHandle pos, NYql::TExprContext& ctx, bool generatedLookup,
    const NYql::NNodes::TCoAtomList* insertOnlyColumns = nullptr);

// Appends DEFAULT-expression column values to every row of the input stream by applying each
// compiled lambda to an empty struct: a DEFAULT expression has no dependencies to read
std::pair<NYql::NNodes::TExprBase, NYql::NNodes::TCoAtomList> ExtendInputRowsWithDefaultExpressionColumns(
    const NYql::NNodes::TExprBase& input, const NYql::NNodes::TCoAtomList& inputColumns,
    const NYql::NNodes::TCoAtomList& defaultExprColumns, const NYql::TKikimrTableDescription& table,
    NYql::TPositionHandle pos, NYql::TExprContext& ctx);

}   // namespace NKikimr::NKqp::NOpt
