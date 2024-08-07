#include "kqp_executer_impl.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/kqp/runtime/kqp_transport.h>

#include <ydb/public/api/protos/ydb_rate_limiter.pb.h>

#include <ydb/library/yql/dq/runtime/dq_transport.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

void TEvKqpExecuter::TEvTxResponse::InitTxResult(const TKqpPhyTxHolder::TConstPtr& tx) {
    TxHolders.push_back(tx);
    TxResults.reserve(TxResults.size() + tx->ResultsSize());

    for (ui32 i = 0; i < tx->ResultsSize(); ++i) {
        const auto& result = tx->GetResults(i);
        const auto& resultMeta = tx->GetTxResultsMeta()[i];

        TMaybe<ui32> queryResultIndex;
        if (result.HasQueryResultIndex()) {
            queryResultIndex = result.GetQueryResultIndex();
        }

        TxResults.emplace_back(result.GetIsStream(), resultMeta.MkqlItemType, &resultMeta.ColumnOrder,
            queryResultIndex);
    }
}

void TEvKqpExecuter::TEvTxResponse::TakeResult(ui32 idx, NDq::TDqSerializedBatch&& rows) {
    YQL_ENSURE(idx < TxResults.size());
    YQL_ENSURE(AllocState);
    ResultRowsCount += rows.RowCount();
    ResultRowsBytes += rows.Size();
    auto guard = AllocState->TypeEnv.BindAllocator();
    auto& result = TxResults[idx];
    if (rows.RowCount() || !result.IsStream) {
        NDq::TDqDataSerializer dataSerializer(
            AllocState->TypeEnv, AllocState->HolderFactory,
            static_cast<NDqProto::EDataTransportVersion>(rows.Proto.GetTransportVersion()));
        dataSerializer.Deserialize(std::move(rows), result.MkqlItemType, result.Rows);
    }
}

TEvKqpExecuter::TEvTxResponse::~TEvTxResponse() {
    if (!TxResults.empty() && Y_LIKELY(AllocState)) {
        with_lock(*AllocState->Alloc) {
            TxResults.crop(0);
        }
    }
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
    const NKikimrConfig::TTableServiceConfig tableServiceConfig, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TPreparedQueryHolder::TConstPtr preparedQuery, const TActorId& creator,
    const TIntrusivePtr<TUserRequestContext>& userRequestContext,
    const bool enableOlapSink, const bool useEvWrite, ui32 statementResultIndex,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup, const TGUCSettings::TPtr& GUCSettings)
{
    if (request.Transactions.empty()) {
        // commit-only or rollback-only data transaction
        return CreateKqpDataExecuter(
            std::move(request), database, userToken, counters, false, tableServiceConfig,
            std::move(asyncIoFactory), creator, 
            userRequestContext, enableOlapSink, useEvWrite, statementResultIndex, 
            federatedQuerySetup, /*GUCSettings*/nullptr
        );
    }

    TMaybe<NKqpProto::TKqpPhyTx::EType> txsType;
    for (auto& tx : request.Transactions) {
        if (txsType) {
            YQL_ENSURE(*txsType == tx.Body->GetType(), "Mixed physical tx types in executer.");
            YQL_ENSURE((*txsType == NKqpProto::TKqpPhyTx::TYPE_DATA)
                || (*txsType == NKqpProto::TKqpPhyTx::TYPE_GENERIC),
                "Cannot execute multiple non-data physical txs.");
        } else {
            txsType = tx.Body->GetType();
        }
    }

    switch (*txsType) {
        case NKqpProto::TKqpPhyTx::TYPE_COMPUTE:
        case NKqpProto::TKqpPhyTx::TYPE_DATA:
            return CreateKqpDataExecuter(
                std::move(request), database, userToken, counters, false, tableServiceConfig,
                std::move(asyncIoFactory), creator, 
                userRequestContext, enableOlapSink, useEvWrite, statementResultIndex, 
                federatedQuerySetup, /*GUCSettings*/nullptr
            );

        case NKqpProto::TKqpPhyTx::TYPE_SCAN:
            return CreateKqpScanExecuter(
                std::move(request), database, userToken, counters,
                tableServiceConfig, preparedQuery, userRequestContext, 
                statementResultIndex
            );

        case NKqpProto::TKqpPhyTx::TYPE_GENERIC:
            return CreateKqpDataExecuter(
                std::move(request), database, userToken, counters, true,
                tableServiceConfig, std::move(asyncIoFactory), creator,
                userRequestContext, enableOlapSink, useEvWrite, statementResultIndex,
                federatedQuerySetup, GUCSettings
            );

        default:
            YQL_ENSURE(false, "Unsupported physical tx type: " << (ui32)*txsType);
    }
}

} // namespace NKqp
} // namespace NKikimr
