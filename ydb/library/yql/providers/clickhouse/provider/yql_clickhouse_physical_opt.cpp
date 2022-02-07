#include "yql_clickhouse_provider_impl.h"

#include <ydb/library/yql/providers/clickhouse/expr_nodes/yql_clickhouse_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>
#include <ydb/library/yql/providers/common/transform/yql_optimize.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NYql {

using namespace NNodes;

namespace {

ui8 GetTypeWeight(const TTypeAnnotationNode& type) {
    switch (type.GetKind()) {
        case ETypeAnnotationKind::Data:
            switch (type.Cast<TDataExprType>()->GetSlot()) {
                case NUdf::EDataSlot::Bool:
                case NUdf::EDataSlot::Int8:
                case NUdf::EDataSlot::Uint8: return 1;

                case NUdf::EDataSlot::Int16:
                case NUdf::EDataSlot::Uint16:
                case NUdf::EDataSlot::Date: return 2;

                case NUdf::EDataSlot::TzDate: return 3;

                case NUdf::EDataSlot::Int32:
                case NUdf::EDataSlot::Uint32:
                case NUdf::EDataSlot::Float:
                case NUdf::EDataSlot::Datetime: return 4;

                case NUdf::EDataSlot::TzDatetime: return 5;

                case NUdf::EDataSlot::Int64:
                case NUdf::EDataSlot::Uint64:
                case NUdf::EDataSlot::Double:
                case NUdf::EDataSlot::Timestamp:
                case NUdf::EDataSlot::Interval:  return 8;

                case NUdf::EDataSlot::TzTimestamp: return 9;

                case NUdf::EDataSlot::Decimal: return 15;
                case NUdf::EDataSlot::Uuid: return 16;

                default: return 32;
            }
        case ETypeAnnotationKind::Optional: return 1 + GetTypeWeight(*type.Cast<TOptionalExprType>()->GetItemType());
        default: return 255;
    }
}

TStringBuf GetLightColumn(const TStructExprType& type) {
    ui8 weight = 255;
    const TItemExprType* field = nullptr;
    for (const auto& item : type.GetItems()) {

        if (const auto w = GetTypeWeight(*item->GetItemType()); w < weight) {
            weight = w;
            field = item;
        }
    }
    return field->GetName();
}

class TClickHousePhysicalOptProposalTransformer : public TOptimizeTransformerBase {
public:
    TClickHousePhysicalOptProposalTransformer(TClickHouseState::TPtr state)
        : TOptimizeTransformerBase(state->Types, NLog::EComponent::ProviderYdb, {})
        , State_(state)
    {
#define HNDL(name) "PhysicalOptimizer-"#name, Hndl(&TClickHousePhysicalOptProposalTransformer::name)
        AddHandler(0, &TCoNarrowMap::Match, HNDL(ReadZeroColumns));
#undef HNDL
    }

    TMaybeNode<TExprBase> ReadZeroColumns(TExprBase node, TExprContext& ctx) const {
        const auto& narrow = node.Maybe<TCoNarrowMap>();
        if (const auto& wide = narrow.Cast().Input().Maybe<TDqReadWideWrap>()) {
            if (const auto& maybe = wide.Cast().Input().Maybe<TClReadTable>()) {
                if (!wide.Cast().Ref().GetTypeAnn()->Cast<TFlowExprType>()->GetItemType()->Cast<TMultiExprType>()->GetSize()) {
                    const auto& read = maybe.Cast();
                    const auto structType = State_->Tables[std::make_pair(read.DataSource().Cluster().Value(), read.Table().Value())].ItemType;
                    auto columns = ctx.NewList(read.Pos(), {ctx.NewAtom(read.Pos(), GetLightColumn(*structType))});
                    return Build<TCoNarrowMap>(ctx, narrow.Cast().Pos())
                        .Input<TDqReadWideWrap>()
                            .InitFrom(wide.Cast())
                            .Input<TClReadTable>()
                                .InitFrom(read)
                                .Columns(std::move(columns))
                            .Build()
                        .Build()
                        .Lambda()
                            .Args({"stub"})
                            .Body<TCoAsStruct>().Build()
                        .Build()
                    .Done();
                }
            }
        }

        return node;
    }
private:
    const TClickHouseState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateClickHousePhysicalOptProposalTransformer(TClickHouseState::TPtr state) {
    return MakeHolder<TClickHousePhysicalOptProposalTransformer>(state);
}

} // namespace NYql


