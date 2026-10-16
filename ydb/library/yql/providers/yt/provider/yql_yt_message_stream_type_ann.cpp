#include "yql_yt_message_stream_impl.h"
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_message_stream_expr_nodes.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/public/udf/udf_data_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <util/string/builder.h>
#include <optional>

namespace NYql {
using namespace NNodes;
namespace {
std::optional<NUdf::EDataSlot> GetYqlSlot(const NYT::NTableClient::TColumnSchema& column) {
    using namespace NYT::NTableClient;
    const TLogicalType* type = column.LogicalType().Get();
    if (type->GetMetatype() == ELogicalMetatype::Optional) {
        type = type->GetElement().Get();
    }
    if (type->GetMetatype() != ELogicalMetatype::Simple) {
        return std::nullopt;
    }
    switch (type->AsSimpleTypeRef().GetElement()) {
        case ESimpleLogicalValueType::Int8: return NUdf::EDataSlot::Int8;
        case ESimpleLogicalValueType::Int16: return NUdf::EDataSlot::Int16;
        case ESimpleLogicalValueType::Int32: return NUdf::EDataSlot::Int32;
        case ESimpleLogicalValueType::Int64: return NUdf::EDataSlot::Int64;
        case ESimpleLogicalValueType::Uint8: return NUdf::EDataSlot::Uint8;
        case ESimpleLogicalValueType::Uint16: return NUdf::EDataSlot::Uint16;
        case ESimpleLogicalValueType::Uint32: return NUdf::EDataSlot::Uint32;
        case ESimpleLogicalValueType::Uint64: return NUdf::EDataSlot::Uint64;
        case ESimpleLogicalValueType::Utf8: return NUdf::EDataSlot::Utf8;
        default: return std::nullopt;
    }
}

const TStructExprType* MakeYtMessageStreamRowType(TExprContext& ctx, const TYtMessageStreamState& state,
    const TString& cluster, const TString& path, TPositionHandle pos) {
    const auto& schema = state.Schemas.at(std::make_pair(cluster, path)).Columns();
    TVector<const TItemExprType*> items;
    items.reserve(schema.size());
    for (const auto& column : schema) {
        const auto slot = GetYqlSlot(column);
        if (!slot) {
            ctx.AddError(TIssue(ctx.GetPosition(pos),
                TStringBuilder() << "Unsupported YT queue column type for '" << column.Name()
                << "': only integer types and UTF-8 strings are supported"));
            return nullptr;
        }
        const TTypeAnnotationNode* type = ctx.MakeType<TDataExprType>(*slot);
        if (!column.Required()) {
            type = ctx.MakeType<TOptionalExprType>(type);
        }
        items.push_back(ctx.MakeType<TItemExprType>(column.Name(), type));
    }
    return ctx.MakeType<TStructExprType>(items);
}

class TYtMessageStreamTypeAnnotation final : public TVisitorTransformerBase {
public:
    explicit TYtMessageStreamTypeAnnotation(std::shared_ptr<TYtMessageStreamState> state)
        : TVisitorTransformerBase(true), State_(std::move(state)) {
        AddHandler({TYtMessageStreamReadTable::CallableName()}, Hndl(&TYtMessageStreamTypeAnnotation::Read));
        AddHandler({TYtMessageStreamSourceSettings::CallableName()}, Hndl(&TYtMessageStreamTypeAnnotation::Settings));
    }
private:
    TStatus Read(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 5, ctx) || !EnsureWorldType(*input->Child(0), ctx)
            || !EnsureSpecificDataSource(*input->Child(1), YtProviderName, ctx)
            || !EnsureAtom(*input->Child(2), ctx) || !EnsureAtom(*input->Child(4), ctx)) {
            return TStatus::Error;
        }
        const auto cluster = TYtMessageStreamDataSource(input->Child(1)).Cluster().StringValue();
        const auto path = TString(input->Child(2)->Content());
        const auto* rowType = MakeYtMessageStreamRowType(ctx, *State_, cluster, path, input->Pos());
        if (!rowType) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TTupleExprType>(TTypeAnnotationNode::TListType{
            input->Child(0)->GetTypeAnn(), ctx.MakeType<TListExprType>(rowType)}));
        return TStatus::Ok;
    }
    TStatus Settings(const TExprNode::TPtr& input, TExprContext& ctx) {
        if (!EnsureArgsCount(*input, 6, ctx) || !EnsureWorldType(*input->Child(0), ctx)
            || !EnsureAtom(*input->Child(1), ctx) || !EnsureAtom(*input->Child(4), ctx)
            || !EnsureAtom(*input->Child(5), ctx)) {
            return TStatus::Error;
        }
        input->SetTypeAnn(ctx.MakeType<TStreamExprType>(ctx.MakeType<TDataExprType>(NUdf::EDataSlot::String)));
        return TStatus::Ok;
    }
    std::shared_ptr<TYtMessageStreamState> State_;
};

}
THolder<TVisitorTransformerBase> CreateYtMessageStreamTypeAnnotation(std::shared_ptr<TYtMessageStreamState> state) {
    return MakeHolder<TYtMessageStreamTypeAnnotation>(std::move(state));
}
}
