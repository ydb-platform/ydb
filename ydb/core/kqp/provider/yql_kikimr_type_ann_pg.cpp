#include "yql_kikimr_type_ann_pg.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <util/string/join.h>

namespace NYql {
namespace NPgTypeAnn {
    
using namespace NNodes;

bool IsPgInsert(const TKiWriteTable &node, TYdbOperation op) {
    if (auto pgSelect = node.Input().Maybe<TCoPgSelect>()) {
        return op == TYdbOperation::InsertAbort
            && NCommon::NeedToRenamePgSelectColumns(pgSelect.Cast());
    }
    return false;
}

bool ValidatePgUpdateKeys(const TKiWriteTable& node, const TKikimrTableDescription* table, TExprContext& ctx) {
    bool ok = true;
    auto updateKeyCheck = [&](const TStringBuf& colName) {
        if (table->GetKeyColumnIndex(TString(colName))) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder()
                << "Cannot update primary key column: " << colName));
            ok = false;
        }
    };
    auto pgSelect = node.Input().Cast<TCoPgSelect>();
    NCommon::TransformPgSetItemOption(pgSelect, "result", [&updateKeyCheck](const TExprBase& setItemOptionNode) {
        auto setItemOption = setItemOptionNode.Cast<TCoNameValueTuple>();
        auto resultList = setItemOption.Value().Cast<TExprList>();
        for (size_t id = 1; id < resultList.Size(); id++) {
            auto pgResultNode = resultList.Item(id).Cast<TCoPgResultItem>();
            if (pgResultNode.ExpandedColumns().Maybe<TExprList>()) {
                auto list = pgResultNode.ExpandedColumns().Cast<TExprList>();
                for (const auto& item : list) {
                    updateKeyCheck(item.Cast<TCoAtom>().Value());
                }
            } else {
                updateKeyCheck(pgResultNode.ExpandedColumns().Cast<TCoAtom>().Value());
            }
        }
    });
    return ok;
}
} //namespace NPgTypeAnn
} //namespace NYql
