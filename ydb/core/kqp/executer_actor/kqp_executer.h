#pragma once

#include <library/cpp/lwtrace/shuttle.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/core/kqp/query_data/kqp_query_data.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

namespace NKikimr {
namespace NKqp {

struct TEvKqpExecuter {
    struct TEvTxRequest : public TEventPB<TEvTxRequest, NKikimrKqp::TEvExecuterTxRequest,
        TKqpExecuterEvents::EvTxRequest> {};

    struct TEvTxResponse : public TEventLocal<TEvTxResponse, TKqpExecuterEvents::EvTxResponse> {
        NKikimrKqp::TEvExecuterTxResponse Record;
        TTxAllocatorState::TPtr AllocState;
        NLongTxService::TLockHandle LockHandle;
        TVector<TKqpPhyTxHolder::TConstPtr> TxHolders;
        TVector<TKqpExecuterTxResult> TxResults;

        NLWTrace::TOrbit Orbit;
        ui64 ResultRowsCount = 0;
        ui64 ResultRowsBytes = 0;

        explicit TEvTxResponse(TTxAllocatorState::TPtr allocState)
            : AllocState(std::move(allocState))
        {}

        ~TEvTxResponse();

        TVector<TKqpPhyTxHolder::TConstPtr>& GetTxHolders() { return TxHolders; }
        TVector<TKqpExecuterTxResult>& GetTxResults() { return TxResults; }
        void InitTxResult(const TKqpPhyTxHolder::TConstPtr& tx);
        void TakeResult(ui32 idx, NKikimr::NMiniKQL::TUnboxedValueVector& rows);
        void TakeResult(ui32 idx, const NYql::NDq::TDqSerializedBatch& rows);

        ui64 GetResultRowsCount() const {
            return ResultRowsCount;
        }

        ui64 GetByteSize() {
            return Record.MutableResponse()->ByteSizeLong() + ResultRowsBytes;
        }

        size_t ResultsSize() const {
            return TxResults.size();
        }
    };

    struct TEvStreamData : public TEventPB<TEvStreamData, NKikimrKqp::TEvExecuterStreamData,
        TKqpExecuterEvents::EvStreamData> {};

    struct TEvStreamDataAck : public TEventPB<TEvStreamDataAck, NKikimrKqp::TEvExecuterStreamDataAck,
        TKqpExecuterEvents::EvStreamDataAck> {};

    struct TEvStreamProfile : public TEventPB<TEvStreamProfile, NKikimrKqp::TEvExecuterStreamProfile,
        TKqpExecuterEvents::EvStreamProfile> {};

    // deprecated event, remove in the future releases.
    struct TEvExecuterProgress : public TEventPB<TEvExecuterProgress, NKikimrKqp::TEvExecuterProgress,
            TKqpExecuterEvents::EvProgress> {};

    struct TEvTableResolveStatus : public TEventLocal<TEvTableResolveStatus,
        TKqpExecuterEvents::EvTableResolveStatus>
    {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;
        TDuration CpuTime;
    };

    struct TEvShardsResolveStatus : public TEventLocal<TEvShardsResolveStatus,
        TKqpExecuterEvents::EvShardsResolveStatus>
    {
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        NYql::TIssues Issues;

        TMap<ui64, ui64> ShardNodes;
        ui32 Unresolved = 0;
    };
};

IActor* CreateKqpExecuter(IKqpGateway::TExecPhysicalRequest&& request, const TString& database,
    const TIntrusiveConstPtr<NACLib::TUserToken>& userToken, TKqpRequestCounters::TPtr counters,
    const NKikimrConfig::TTableServiceConfig::TAggregationConfig& aggregation,
    const NKikimrConfig::TTableServiceConfig::TExecuterRetriesConfig& executerRetriesConfig,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory, TPreparedQueryHolder::TConstPtr preparedQuery,
    const TActorId& creator);

IActor* CreateKqpSchemeExecuter(TKqpPhyTxHolder::TConstPtr phyTx, const TActorId& target, const TString& database,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, NKikimr::NKqp::TTxAllocatorState::TPtr txAlloc);

std::unique_ptr<TEvKqpExecuter::TEvTxResponse> ExecuteLiteral(
    IKqpGateway::TExecPhysicalRequest&& request, TKqpRequestCounters::TPtr counters, TActorId owner);

} // namespace NKqp
} // namespace NKikimr
