#pragma once

#include <library/cpp/lwtrace/shuttle.h>
#include <ydb/core/kqp/common/kqp_common.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/tx/long_tx_service/public/lock_handle.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/kqp.pb.h>

namespace NKikimr {
namespace NKqp {

struct TEvKqpExecuter {
    struct TEvTxRequest : public TEventPB<TEvTxRequest, NKikimrKqp::TEvExecuterTxRequest,
        TKqpExecuterEvents::EvTxRequest> {};

    struct TEvTxResponse : public TEventPB<TEvTxResponse, NKikimrKqp::TEvExecuterTxResponse,
        TKqpExecuterEvents::EvTxResponse>
    {
        NLongTxService::TLockHandle LockHandle;

        NLWTrace::TOrbit Orbit;

        bool IsSerializable() const override {
            // We cannot serialize LockHandle, should always send locally
            return false;
        }
    };

    struct TEvStreamData : public TEventPB<TEvStreamData, NKikimrKqp::TEvExecuterStreamData,
        TKqpExecuterEvents::EvStreamData> {};

    struct TEvStreamDataAck : public TEventPB<TEvStreamDataAck, NKikimrKqp::TEvExecuterStreamDataAck,
        TKqpExecuterEvents::EvStreamDataAck> {};

    struct TEvStreamProfile : public TEventPB<TEvStreamProfile, NKikimrKqp::TEvExecuterStreamProfile,
        TKqpExecuterEvents::EvStreamProfile> {};

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
    const TMaybe<TString>& userToken, TKqpRequestCounters::TPtr counters);

} // namespace NKqp
} // namespace NKikimr
