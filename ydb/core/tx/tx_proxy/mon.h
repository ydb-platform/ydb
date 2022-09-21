#pragma once
#include "defs.h"

#include <ydb/core/mon/mon.h>
#include <ydb/library/yql/minikql/aligned_page_pool.h>

namespace NKikimr {
namespace NTxProxy {

    ////////////////////////////////////////////////////////////////////////////
    // TX PROXY MONITORING COUNTERS
    ////////////////////////////////////////////////////////////////////////////
    struct TTxProxyMon : public TThrRefBase {

        TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

        // tx_proxy group
        TIntrusivePtr<::NMonitoring::TDynamicCounters> TxGroup;

        NMonitoring::THistogramPtr CacheRequestLatency;

        ::NMonitoring::TDynamicCounters::TCounterPtr Navigate;
        NMonitoring::THistogramPtr NavigateLatency;
        ::NMonitoring::TDynamicCounters::TCounterPtr SchemeRequest;
        NMonitoring::THistogramPtr SchemeRequestLatency;
        ::NMonitoring::TDynamicCounters::TCounterPtr MakeRequest;
        //NMonitoring::THistogramPtr MakeRequestLatency;
        ::NMonitoring::TDynamicCounters::TCounterPtr SnapshotRequest;
        ::NMonitoring::TDynamicCounters::TCounterPtr CommitWritesRequest;

        ::NMonitoring::TDynamicCounters::TCounterPtr SchemeRequestProxyNotReady;
        ::NMonitoring::TDynamicCounters::TCounterPtr MakeRequestProxyNotReady;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxNotImplemented;
        ::NMonitoring::TDynamicCounters::TCounterPtr KqpRequest;

        ::NMonitoring::TDynamicCounters::TCounterPtr DataReqInFly;
        ::NMonitoring::TDynamicCounters::TCounterPtr SchemeReqInFly;
        ::NMonitoring::TDynamicCounters::TCounterPtr NavigateReqInFly;

        // tx_proxy_datareq group
        TIntrusivePtr<::NMonitoring::TDynamicCounters> DataReqGroup;

        ::NMonitoring::TDynamicCounters::TCounterPtr ReportStatusOK;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReportStatusNotOK;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReportStatusStreamData;

        NMonitoring::THistogramPtr TxPrepareTimeHgram;
        NMonitoring::THistogramPtr TxExecuteTimeHgram;
        NMonitoring::THistogramPtr TxTotalTimeHgram;
        NMonitoring::THistogramPtr TxPrepareSpreadHgram;
        NMonitoring::THistogramPtr TxPrepareArriveSpreadHgram;
        NMonitoring::THistogramPtr TxPrepareCompleteSpreadHgram;
        NMonitoring::THistogramPtr TxExecSpreadHgram;

        NMonitoring::THistogramPtr TxPrepareSetProgramHgram;
        NMonitoring::THistogramPtr TxPrepareResolveHgram;
        NMonitoring::THistogramPtr TxPrepareBuildShardProgramsHgram;
        NMonitoring::THistogramPtr TxPrepareSendShardProgramsHgram;

        ::NMonitoring::TDynamicCounters::TCounterPtr MiniKQLResolveSentToShard;
        ::NMonitoring::TDynamicCounters::TCounterPtr MiniKQLWrongRequest;

        ::NMonitoring::TDynamicCounters::TCounterPtr ReadTableResolveSentToShard;
        ::NMonitoring::TDynamicCounters::TCounterPtr ReadTableWrongRequest;

        ::NMonitoring::TDynamicCounters::TCounterPtr MiniKQLProgramSize;
        ::NMonitoring::TDynamicCounters::TCounterPtr MiniKQLParamsSize;

        ::NMonitoring::TDynamicCounters::TCounterPtr ExecTimeout;
        ::NMonitoring::TDynamicCounters::TCounterPtr MarkShardError;
        ::NMonitoring::TDynamicCounters::TCounterPtr PrepareErrorTimeout;

        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientDestroyed;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientConnected;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanCoordinatorDeclined;

        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientTxResultComplete;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientTxResultAborted;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientTxResultResultUnavailable;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientTxResultCancelled;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanClientTxResultExecError;

        ::NMonitoring::TDynamicCounters::TCounterPtr ClientTxStatusAccepted;
        ::NMonitoring::TDynamicCounters::TCounterPtr ClientTxStatusProcessed;
        ::NMonitoring::TDynamicCounters::TCounterPtr ClientTxStatusConfirmed;
        ::NMonitoring::TDynamicCounters::TCounterPtr ClientTxStatusPlanned;
        ::NMonitoring::TDynamicCounters::TCounterPtr ClientTxStatusCoordinatorDeclined;

        ::NMonitoring::TDynamicCounters::TCounterPtr MakeRequestProxyAccepted;
        ::NMonitoring::TDynamicCounters::TCounterPtr MakeRequestWrongRequest;
        ::NMonitoring::TDynamicCounters::TCounterPtr MakeRequestEmptyAffectedSet;

        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetLegacySuccess;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetMiniKQLSuccess;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetReadTableSuccess;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetFail;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetWrongRequest;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetDomainLocalityFail;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResolveKeySetRedirectUnavaible;

        ::NMonitoring::TDynamicCounters::TCounterPtr ClientConnectedOk;
        ::NMonitoring::TDynamicCounters::TCounterPtr ClientConnectedError;

        ::NMonitoring::TDynamicCounters::TCounterPtr ClientDestroyedOk;
        ::NMonitoring::TDynamicCounters::TCounterPtr ClientDestroyedError;

        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultTabletPrepared;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultPrepared;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultComplete;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultError;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultAborted;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultFatal;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultShardOverloaded;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultShardTryLater;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultExecError;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultResultUnavailable;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxResultCancelled;

        ::NMonitoring::TDynamicCounters::TCounterPtr MergeResultRequestExecError;
        ::NMonitoring::TDynamicCounters::TCounterPtr MergeResultRequestExecComplete;
        ::NMonitoring::TDynamicCounters::TCounterPtr MergeResultMiniKQLExecError;
        ::NMonitoring::TDynamicCounters::TCounterPtr MergeResultMiniKQLExecComplete;
        ::NMonitoring::TDynamicCounters::TCounterPtr MergeResultMiniKQLUnknownStatus;

        ::NMonitoring::TDynamicCounters::TCounterPtr ResultsReceivedCount;
        ::NMonitoring::TDynamicCounters::TCounterPtr ResultsReceivedSize;

        TAlignedPagePoolCounters AllocPoolCounters;

        TTxProxyMon(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);
    };

} // NTxProxy
} // NKikimr
