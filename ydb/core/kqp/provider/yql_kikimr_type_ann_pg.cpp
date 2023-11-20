#include "yql_kikimr_type_ann_pg.h"

#include <util/string/join.h>

namespace NYql {
namespace NPgTypeAnn {
    
using namespace NNodes;

namespace {
    bool MatchesSetItemOption(const TExprBase& setItemOption, TStringBuf name) {
        if (setItemOption.Ref().IsList() && setItemOption.Ref().ChildrenSize() > 0) {
            if (setItemOption.Ref().ChildPtr(0)->Content() == name) {
                return true;
            }
        }
        return false;
    }

    TExprNode::TPtr GetSetItemOptionValue(const TExprBase& setItemOption) {
        if (setItemOption.Ref().IsList() && setItemOption.Ref().ChildrenSize() > 1) {
            return setItemOption.Ref().ChildPtr(1);
        }
        return nullptr;
    }

    bool TransformPgSetItemOption(
        const TKiWriteTable& node, 
        TStringBuf optionName, 
        std::function<void(const TExprBase&)> lambda
    ) {
        bool applied = false;
        if (auto pgSelect = node.Input().Maybe<TCoPgSelect>()) {    
            for (const auto& option : pgSelect.Cast().SelectOptions()) {
                if (option.Name() == "set_items") {
                    auto pgSetItems = option.Value().Cast<TExprList>();
                    for (const auto& setItem : pgSetItems) {
                        auto setItemNode = setItem.Cast<TCoPgSetItem>();
                        for (const auto& setItemOption : setItemNode.SetItemOptions()) {
                            if (MatchesSetItemOption(setItemOption, optionName)) {
                                applied = true;
                                lambda(setItemOption);
                            }
                        }
                    }
                }
            }
        }
        return applied;
    }

    TExprNode::TPtr GetSetItemOption(const TKiWriteTable& node, TStringBuf optionName) {
        TExprNode::TPtr nodePtr = nullptr;
        TransformPgSetItemOption(node, optionName, [&nodePtr](const TExprBase& option) {
            nodePtr = option.Ptr();
        });
        return nodePtr;
    }
} //namespace

bool NeedsValuesRename(const NNodes::TKiWriteTable &node, TYdbOperation op) {
    auto fill = GetSetItemOption(node, "fill_target_columns");

    return op == TYdbOperation::InsertAbort
        && fill
        && !GetSetItemOptionValue(TExprBase(fill));
}

bool RewriteValuesColumnNames(
    const TKiWriteTable& node, 
    const TKikimrTableDescription* table, 
    TExprContext& ctx, 
    TTypeAnnotationContext& types
) {
    bool ok = true;
    TransformPgSetItemOption(node, "values", [&ok, &node, &table, &ctx, &types](const TExprBase &setItemOption) {
        auto values = GetSetItemOptionValue(setItemOption);
        if (values->ChildrenSize() > table->Metadata->Columns.size()) {
            ctx.AddError(TIssue(ctx.GetPosition(node.Pos()), TStringBuilder() << Sprintf(
                "VALUES have %zu columns, INSERT INTO expects: %zu", 
                values->ChildrenSize(), 
                table->Metadata->Columns.size()
            )));
            ok = false;
            return;
        }
        TExprNode::TListType columns;
        THashMap<TString, TString> valueColumnName;
        columns.reserve(values->ChildrenSize()); 
        for (ui32 index = 0; index < values->ChildrenSize(); ++index) {
            valueColumnName[values->Child(index)->Content()] = table->Metadata->ColumnOrder.at(index);
            columns.push_back(Build<TCoAtom>(ctx, node.Pos())
                .Value(table->Metadata->ColumnOrder.at(index))
            .Done().Ptr());
            columns.back()->SetTypeAnn(values->Child(index)->GetTypeAnn());
        }
        values->ChangeChildrenInplace(std::move(columns));
        auto input = node.Ptr()->ChildRef(TKiWriteTable::idx_Input);
        const TTypeAnnotationNode* inputType;
        switch (input->GetTypeAnn()->GetKind()) {
            case ETypeAnnotationKind::List:
                inputType = input->GetTypeAnn()->Cast<TListExprType>()->GetItemType();
                break;
            default:
                inputType = input->GetTypeAnn();
                break;
        }
        Y_ENSURE(inputType->GetKind() == ETypeAnnotationKind::Struct);
        TVector<const TItemExprType*> rowTypeItems;
        for (const auto& item : inputType->Cast<TStructExprType>()->GetItems()) {
            rowTypeItems.emplace_back(ctx.MakeType<TItemExprType>(valueColumnName[item->GetName()], item->GetItemType()));
        }  
        const TStructExprType* rowStructType = ctx.MakeType<TStructExprType>(rowTypeItems);
        if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::List) {
            input->SetTypeAnn(ctx.MakeType<TListExprType>(rowStructType));
        } else {
            input->SetTypeAnn(rowStructType);
        }
        types.SetColumnOrder(*input, TVector<TString>(table->Metadata->ColumnOrder.begin(), table->Metadata->ColumnOrder.begin() + values->ChildrenSize()), ctx, /*overwrite=*/true);  
    });
    if (ok) {
        auto fill = GetSetItemOption(node, "fill_target_columns");
        fill->ChangeChildrenInplace({
            fill->Child(0),
            Build<TCoAtom>(ctx, node.Pos())
                .Value("done")
            .Done().Ptr()
        });
        fill->ChildPtr(1)->SetTypeAnn(ctx.MakeType<TUnitExprType>());
    }
    return ok;
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
    TransformPgSetItemOption(node, "result", [&updateKeyCheck](const TExprBase& setItemOptionNode) {
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
