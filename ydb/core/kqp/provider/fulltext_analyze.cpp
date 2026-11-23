#include "fulltext_analyze.h"

#include <ydb/core/base/fulltext.h>

#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

using namespace NNodes;

TExprNode::TPtr BuildFulltextAnalyze(const TKikimrTableDescription& table, const TExprNode::TPtr& inputRow,
    const TIndexDescription& index, TPositionHandle pos, TExprContext& ctx)
{
    using NKikimr::NFulltext::EIndexMode;

    TString settingsProto;
    TString textColumn;
    EIndexMode mode = EIndexMode::Invalid;

    if (index.Type == TIndexDescription::EType::GlobalJson) {
        YQL_ENSURE(!index.KeyColumns.empty(), "Expected at least one key column in JSON index");
        textColumn = index.KeyColumns.back();

        auto columnType = table.GetColumnType(textColumn);
        YQL_ENSURE(columnType, "Failed to get column type for JSON index column");

        if (columnType->GetKind() == ETypeAnnotationKind::Optional) {
            columnType = columnType->Cast<TOptionalExprType>()->GetItemType();
        }

        YQL_ENSURE(columnType->GetKind() == ETypeAnnotationKind::Data,
            "Expected data type for JSON index column");

        switch (columnType->Cast<TDataExprType>()->GetSlot()) {
            case EDataSlot::Json:
                mode = EIndexMode::JsonIndexOverJson;
                break;
            case EDataSlot::JsonDocument:
                mode = EIndexMode::JsonIndexOverJsonDocument;
                break;
            default:
                YQL_ENSURE(false, "Unexpected data slot for JSON index column");
        }
    } else {
        const auto* description = std::get_if<NKikimrSchemeOp::TFulltextIndexDescription>(&index.SpecializedIndexDescription);
        YQL_ENSURE(description, "Expected fulltext index description");

        const auto& columns = description->GetSettings().columns();
        YQL_ENSURE(columns.size() == 1, "Expected single text column in fulltext index");
        textColumn = columns.at(0).column();
        YQL_ENSURE(columns.at(0).analyzers().SerializeToString(&settingsProto));
        mode = EIndexMode::Fulltext;
    }

    const auto textMember = Build<TCoMember>(ctx, pos)
        .Struct(TExprBase(inputRow))
        .Name().Build(textColumn)
        .Done();
    const auto settingsLiteral = Build<TCoString>(ctx, pos)
        .Literal().Build(settingsProto)
        .Done();

    return ctx.NewCallable(pos, "FulltextAnalyze", {
        textMember.Ptr(), settingsLiteral.Ptr(), ctx.NewAtom(pos, ToString(static_cast<ui32>(mode)))});
}

} // namespace NYql
