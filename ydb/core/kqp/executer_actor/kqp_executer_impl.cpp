#include "kqp_executer_impl.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>

#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>
#include <ydb/library/yql/dq/runtime/dq_arrow_helpers.h>

#include <library/cpp/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

void TEvKqpExecuter::TEvTxResponse::InitTxResult(const TKqpPhyTxHolder::TConstPtr& tx) {
    TxHolders.push_back(tx);
    TxResults.reserve(TxResults.size() + tx->ResultsSize());

    for (ui32 i = 0; i < tx->ResultsSize(); ++i) {
        const auto& result = tx->GetResults(i);
        const auto& resultMeta = tx->GetTxResultsMeta()[i];

        TxResults.emplace_back(result.GetIsStream(), resultMeta.MkqlItemType, &resultMeta.ColumnOrder,
            result.GetQueryResultIndex());
    }
}

void TEvKqpExecuter::TEvTxResponse::TakeResult(ui32 idx, const NYql::NDqProto::TData& rows) {
    YQL_ENSURE(idx < TxResults.size());
    ResultRowsCount += rows.GetRows();
    ResultRowsBytes += rows.GetRaw().size();
    auto guard = AllocState->TypeEnv.BindAllocator();
    auto& result = TxResults[idx];
    if (rows.GetRows() || !result.IsStream) {
        NDq::TDqDataSerializer dataSerializer(
            AllocState->TypeEnv, AllocState->HolderFactory,
            static_cast<NDqProto::EDataTransportVersion>(rows.GetTransportVersion()));
        dataSerializer.Deserialize(rows, result.MkqlItemType, result.Rows);
    }
}

TEvKqpExecuter::TEvTxResponse::~TEvTxResponse() {
    if (!TxResults.empty()) {
        with_lock(AllocState->Alloc) {
            TxResults.crop(0);
        }
    }
}

void TEvKqpExecuter::TEvTxResponse::TakeResult(ui32 idx, NKikimr::NMiniKQL::TUnboxedValueVector& rows) {
    YQL_ENSURE(idx < TxResults.size());
    ResultRowsCount += rows.size();
    auto& txResult = TxResults[idx];
    auto serializer = NYql::NDq::TDqDataSerializer(
        AllocState->TypeEnv, AllocState->HolderFactory, NDqProto::DATA_TRANSPORT_UV_PICKLE_1_0);
    auto buffer = serializer.Serialize(rows.begin(), rows.end(), txResult.MkqlItemType);
    {
        auto g = AllocState->TypeEnv.BindAllocator();
        NKikimr::NMiniKQL::TUnboxedValueVector emptyVector;
        emptyVector.swap(rows);
    }

    serializer.Deserialize(buffer, txResult.MkqlItemType, txResult.Rows);
}

TActorId ReportToRl(ui64 ru, const TString& database, const TString& userToken,
    const NKikimrKqp::TRlPath& path)
{
    Ydb::RateLimiter::AcquireResourceRequest req;
    req.set_coordination_node_path(path.GetCoordinationNode());
    req.set_resource_path(path.GetResourcePath());
    req.set_used(ru);

    // No need to handle result of rate limiter response on the response hook
    // just report ru usage
    auto noop = [](Ydb::RateLimiter::AcquireResourceResponse) {};
    return NKikimr::NRpcService::RateLimiterAcquireUseSameMailbox(
        std::move(req),
        database,
        userToken,
        std::move(noop),
        TActivationContext::AsActorContext());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

IActor* CreateKqpExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, TPreparedQueryHolder::TConstPtr preparedQuery)
{
    if (request.Transactions.empty()) {
        // commit-only or rollback-only data transaction
        YQL_ENSURE(request.EraseLocks);
        return CreateKqpDataExecuter(std::move(request), database, userToken, counters, false, executerRetriesConfig, std::move(asyncIoFactory));
    }

    TMaybe<NKqpProto::TKqpPhyTx::EType> txsType;
    for (auto& tx : request.Transactions) {
        if (txsType) {
            YQL_ENSURE(*txsType == tx.Body->GetType(), "Mixed physical tx types in executer.");
            YQL_ENSURE(*txsType == NKqpProto::TKqpPhyTx::TYPE_DATA, "Cannot execute multiple non-data physical txs.");
        } else {
            txsType = tx.Body->GetType();
        }
    }

    switch (*txsType) {
        case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
        case NKqpProto::TKqpPhyTx::TYPE_DATA:
            return CreateKqpDataExecuter(std::move(request), database, userToken, counters, false, executerRetriesConfig, std::move(asyncIoFactory));

        case NKqpProto::TKqpPhyTx::TYPE_SCAN:
            return CreateKqpScanExecuter(std::move(request), database, userToken, counters, aggregation, executerRetriesConfig, preparedQuery);

        case NKqpProto::TKqpPhyTx::TYPE_GENERIC:
            return CreateKqpDataExecuter(std::move(request), database, userToken, counters, true, executerRetriesConfig, std::move(asyncIoFactory));

        default:
            YQL_ENSURE(false, "Unsupported physical tx type: " << (ui32)*txsType);
    }
}

} // namespace NKqp
} // namespace NKikimr
