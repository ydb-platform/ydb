#include "kqp_transform.h"
#include "kqp_yql.h"

#include <ydb/library/yql/providers/common/provider/yql_provider.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

IGraphTransformer::TStatus TLogExprTransformer::operator()(const TExprNode::TPtr& input, TExprNode::TPtr& output,
    TExprContext& ctx)
{
    Y_UNUSED(ctx);

    output = input;
    LogExpr(*input, ctx, Description, Component, Level);
    return IGraphTransformer::TStatus::Ok;
}

TAutoPtr<IGraphTransformer> TLogExprTransformer::Sync(const TString& description, NLog::EComponent component,
    NLog::ELevel level)
{
    return CreateFunctorTransformer(TLogExprTransformer(description, component, level));
}

void TLogExprTransformer::LogExpr(const TExprNode& input, TExprContext& ctx, const TString& description, NLog::EComponent component,
    NLog::ELevel level)
{
    YQL_CVLOG(level, component) << description << ":\n" << KqpExprToPrettyString(input, ctx);
}

TMaybe<NYql::NDq::TMkqlValueRef> GetParamValue(bool ensure, NYql::TKikimrQueryContext& queryCtx,
    const TVector<TVector<NKikimrMiniKQL::TResult>>& txResults, const NKqpProto::TKqpPhyParamBinding& paramBinding)
{
    switch (paramBinding.GetTypeCase()) {
        case NKqpProto::TKqpPhyParamBinding::kExternalBinding: {
            const auto* clientParam = queryCtx.Parameters.FindPtr(paramBinding.GetName());
            if (clientParam) {
                return NYql::NDq::TMkqlValueRef(*clientParam);
            }
            Y_ENSURE(!ensure || clientParam, "Parameter not found: " << paramBinding.GetName());
            return {};
        }
        case NKqpProto::TKqpPhyParamBinding::kTxResultBinding: {
            auto& txResultBinding = paramBinding.GetTxResultBinding();
            auto txIndex = txResultBinding.GetTxIndex();
            auto resultIndex = txResultBinding.GetResultIndex();

            if (txIndex < txResults.size() && resultIndex < txResults[txIndex].size()) {
                return NYql::NDq::TMkqlValueRef(txResults[txIndex][resultIndex]);
            }

            YQL_ENSURE(!ensure || txIndex < txResults.size());
            YQL_ENSURE(!ensure || resultIndex < txResults[txIndex].size());
            return {};
        }
        case NKqpProto::TKqpPhyParamBinding::kInternalBinding: {
            auto& internalBinding = paramBinding.GetInternalBinding();
            auto& param = queryCtx.Parameters[paramBinding.GetName()];

            switch (internalBinding.GetType()) {
                case NKqpProto::TKqpPhyInternalBinding::PARAM_NOW:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
                    param.MutableValue()->SetUint64(queryCtx.GetCachedNow());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATE: {
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TDate>::Id);
                    ui64 date = queryCtx.GetCachedDate();
                    YQL_ENSURE(date <= Max<ui32>());
                    param.MutableValue()->SetUint32(static_cast<ui32>(date));
                    break;
                }
                case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_DATETIME: {
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TDatetime>::Id);
                    ui64 datetime = queryCtx.GetCachedDatetime();
                    YQL_ENSURE(datetime <= Max<ui32>());
                    param.MutableValue()->SetUint32(static_cast<ui32>(datetime));
                    break;
                }
                case NKqpProto::TKqpPhyInternalBinding::PARAM_CURRENT_TIMESTAMP:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TTimestamp>::Id);
                    param.MutableValue()->SetUint64(queryCtx.GetCachedTimestamp());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_NUMBER:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<ui64>::Id);
                    param.MutableValue()->SetUint64(queryCtx.GetCachedRandom<ui64>());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM:
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<double>::Id);
                    param.MutableValue()->SetDouble(queryCtx.GetCachedRandom<double>());
                    break;
                case NKqpProto::TKqpPhyInternalBinding::PARAM_RANDOM_UUID: {
                    param.MutableType()->SetKind(NKikimrMiniKQL::ETypeKind::Data);
                    param.MutableType()->MutableData()->SetScheme(NKikimr::NUdf::TDataType<NUdf::TUuid>::Id);
                    auto uuid = queryCtx.GetCachedRandom<TGUID>();
                    const auto ptr = reinterpret_cast<ui8*>(uuid.dw);
                    param.MutableValue()->SetLow128(*reinterpret_cast<ui64*>(ptr));
                    param.MutableValue()->SetHi128(*reinterpret_cast<ui64*>(ptr + 8));
                    break;
                }
                default:
                    YQL_ENSURE(false, "Unexpected internal parameter type: " << (ui32)internalBinding.GetType());
            }

            return NYql::NDq::TMkqlValueRef(param);
        }
        default:
            YQL_ENSURE(false, "Unexpected parameter binding type: " << (ui32)paramBinding.GetTypeCase());
    }
}

} // namespace NKqp
} // namespace NKikimr
