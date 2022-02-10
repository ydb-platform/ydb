#include "yql_ydb_provider_impl.h"

#include <ydb/library/yql/providers/common/schema/expr/yql_expr_schema.h>

namespace NYql {

bool TYdbKey::Extract(const TExprNode& key, TExprContext& ctx) {
    if (key.IsCallable("MrTableConcat")) {
        ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "CONCAT is not supported on Ydb clusters."));
        return false;
    }

    if (!key.IsCallable("Key")) {
        ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "Expected key"));
        return false;
    }

    if (const auto& tagName = key.Head().Head().Content(); tagName == "table") {
        KeyType = Type::Table;
        const auto nameNode = key.Head().Child(1);
        if (nameNode->IsCallable({"MrTableRange", "MrTableRangeStrict"})) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "RANGE is not supported on Ydb clusters."));
            return false;
        }

        if (!nameNode->IsCallable("String")) {
            ctx.AddError(TIssue(ctx.GetPosition(key.Pos()), "Expected String as table key."));
            return false;
        }

        Target = nameNode->Head().Content();
    } else if (tagName == "tablescheme") {
        KeyType = Type::TableScheme;
        Target = key.Head().Child(1)->Head().Content();
    } else if (tagName == "tablelist") {
        KeyType = Type::TableList;
        Target = key.Head().Child(1)->Head().Content();
    } else if (tagName == "role") {
        KeyType = Type::Role;
        Target = key.Head().Child(1)->Head().Content();
    } else {
        ctx.AddError(TIssue(ctx.GetPosition(key.Head().Pos()), TString("Unexpected tag for YDB key: ") += tagName));
        return false;
    }

    if (key.ChildrenSize() > 1) {
        for (ui32 i = 1; i < key.ChildrenSize(); ++i) {
            if (const auto& tag = key.Child(i)->Head(); tag.Content() == "view") {
                const auto viewNode = key.Child(i)->Child(1);
                if (!viewNode->IsCallable("String")) {
                    ctx.AddError(TIssue(ctx.GetPosition(viewNode->Pos()), "Expected String"));
                    return false;
                }

                if (viewNode->ChildrenSize() != 1 || !EnsureAtom(viewNode->Head(), ctx)) {
                    ctx.AddError(TIssue(ctx.GetPosition(viewNode->Head().Pos()), "Dynamic views names are not supported"));
                    return false;
                }
                if (viewNode->Head().Content().empty()) {
                    ctx.AddError(TIssue(ctx.GetPosition(viewNode->Head().Pos()), "Secondary index name must not be empty"));
                    return false;
                }
                View = viewNode->Head().Content();

            } else {
                ctx.AddError(TIssue(ctx.GetPosition(tag.Pos()), TString("Unexpected tag for YDB key child: ") += tag.Content()));
                return false;
            }
        }
    }
    return true;
}

void MetaToYson(const TString& cluster, const TString& table,  TYdbState::TPtr state, NYson::TYsonWriter& writer) {
    const auto& meta = state->Tables[std::make_pair(cluster, table)];
    writer.OnBeginMap();
    writer.OnKeyedItem("Data");

    writer.OnBeginMap();
    writer.OnKeyedItem(TStringBuf("Cluster"));
    writer.OnStringScalar(cluster);
    writer.OnKeyedItem(TStringBuf("Name"));
    writer.OnStringScalar(table);


    writer.OnKeyedItem(TStringBuf("DoesExist"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("IsSorted"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("IsDynamic"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("UniqueKeys"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("CanWrite"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("IsRealData"));
    writer.OnBooleanScalar(true);
    writer.OnKeyedItem(TStringBuf("YqlCompatibleSchema"));
    writer.OnBooleanScalar(true);

    writer.OnKeyedItem("Fields");
    writer.OnBeginList();
    {
        for (auto& item: meta.ItemType->GetItems()) {
            writer.OnListItem();

            auto name = item->GetName();
            writer.OnBeginMap();

            writer.OnKeyedItem("Name");
            writer.OnStringScalar(name);

            writer.OnKeyedItem("Type");
            NCommon::WriteTypeToYson(writer, item->GetItemType());

            writer.OnKeyedItem("ClusterSortOrder");
            writer.OnBeginList();
            writer.OnEndList();

            writer.OnKeyedItem("Ascending");
            writer.OnBeginList();

            writer.OnEndList();

            writer.OnEndMap();
        }
    }
    writer.OnEndList();

    writer.OnKeyedItem("RowType");
    NCommon::WriteTypeToYson(writer, meta.ItemType);

    writer.OnEndMap();
    writer.OnEndMap();
}

}
