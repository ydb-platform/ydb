#include "yql_kikimr_expr_nodes.h"

#include "yql_kikimr_provider_impl.h"

namespace NYql {
namespace NNodes {

TString TKiReadTable::GetTable(TExprContext& ctx) const {
    TKikimrKey key(ctx);
    YQL_ENSURE(key.Extract(TableKey().Ref()));
    YQL_ENSURE(key.GetKeyType() == TKikimrKey::Type::Table);

    return key.GetTablePath();
}

TCoAtomList TKiReadTable::GetSelectColumns(TExprContext& ctx, const TKikimrTableDescription& tableData,
    bool withSystemColumns) const
{
    if (Select().Maybe<TCoVoid>()) {
        return BuildColumnsList(tableData, Pos(), ctx, withSystemColumns, true /*ignoreWriteOnlyColumns*/);
    }

    return Select().Cast<TCoAtomList>();
}

TCoAtomList TKiReadTable::GetSelectColumns(TExprContext& ctx, const TKikimrTablesData& tablesData,
    bool withSystemColumns) const
{
    auto& tableData = tablesData.ExistingTable(TString(DataSource().Cluster()), GetTable(ctx));
    return GetSelectColumns(ctx, tableData, withSystemColumns);
}

} // namespace NNodes
} // namespace NYql
