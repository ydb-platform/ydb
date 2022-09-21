#include "mon.h"

#include <ydb/core/base/counters.h>

namespace NKikimr {
namespace NTxProxy {

TTxProxyMon::TTxProxyMon(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters)
    : Counters(counters)
    , TxGroup(GetServiceCounters(counters, "proxy")->GetSubgroup("subsystem", "tx"))
    , DataReqGroup(GetServiceCounters(counters, "proxy")->GetSubgroup("subsystem", "datareq"))
    , AllocPoolCounters(counters, "tx_proxy")
{
    CacheRequestLatency = TxGroup->GetHistogram("CacheRequest/LatencyMs", NMonitoring::ExponentialHistogram(10, 4, 1));

    Navigate = TxGroup->GetCounter("Navigate/Request", true);
    NavigateLatency = TxGroup->GetHistogram("Navigate/LatencyMs", NMonitoring::ExponentialHistogram(10, 4, 1));

    SchemeRequest = TxGroup->GetCounter("Propose/SchemeRequest", true);
    SchemeRequestLatency = TxGroup->GetHistogram("SchemeRequest/LatencyMs", NMonitoring::ExponentialHistogram(10, 4, 1));
    SchemeRequestProxyNotReady = TxGroup->GetCounter("Propose/SchemeRequestProxyNotReady", true);
    MakeRequest = TxGroup->GetCounter("Propose/MakeRequest", true);
    SnapshotRequest = TxGroup->GetCounter("Propose/SnapshotRequest", true);
    CommitWritesRequest = TxGroup->GetCounter("Propose/CommitWritesRequest", true);
    MakeRequestProxyNotReady = TxGroup->GetCounter("Propose/MakeRequestProxyNotReady", true);
    TxNotImplemented = TxGroup->GetCounter("Propose/TxNotImplemented", true);
    KqpRequest = TxGroup->GetCounter("Propose/KqpRequest", true);

    DataReqInFly = TxGroup->GetCounter("InFly/DataTx", false);
    SchemeReqInFly = TxGroup->GetCounter("InFly/SchemeOp", false);
    NavigateReqInFly = TxGroup->GetCounter("InFly/Navigate", false);

    ReportStatusOK = DataReqGroup->GetCounter("ReportStatus/OK", true);
    ReportStatusNotOK = DataReqGroup->GetCounter("ReportStatus/NotOK", true);
    ReportStatusStreamData = DataReqGroup->GetCounter("ReportStatus/StreamData", true);

    TxPrepareTimeHgram = DataReqGroup->GetHistogram("TxPrepareTimesMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));
    TxExecuteTimeHgram = DataReqGroup->GetHistogram("TxExecuteTimesMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));
    TxTotalTimeHgram = DataReqGroup->GetHistogram("TxTotalTimesMs",
        NMonitoring::ExponentialHistogram(20, 2, 1));
    TxPrepareSpreadHgram = DataReqGroup->GetHistogram("TxPrepareSpreadMs",
        NMonitoring::ExponentialHistogram(10, 2, 1));
    TxPrepareArriveSpreadHgram = DataReqGroup->GetHistogram("TxPrepareArriveSpreadMs",
        NMonitoring::ExponentialHistogram(10, 2, 1));
    TxPrepareCompleteSpreadHgram = DataReqGroup->GetHistogram("TxPrepareCompleteSpreadMs",
        NMonitoring::ExponentialHistogram(10, 2, 1));
    TxExecSpreadHgram = DataReqGroup->GetHistogram("TxExecSpreadMs",
        NMonitoring::ExponentialHistogram(10, 2, 1));

    TxPrepareSetProgramHgram = DataReqGroup->GetHistogram("TxPrepareSetProgramUs",
        NMonitoring::ExponentialHistogram(10, 2, 100));
    TxPrepareResolveHgram = DataReqGroup->GetHistogram("TxPrepareResolveUs",
        NMonitoring::ExponentialHistogram(10, 2, 100));
    TxPrepareBuildShardProgramsHgram = DataReqGroup->GetHistogram("TxPrepareBuildShardProgramsUs",
        NMonitoring::ExponentialHistogram(10, 2, 100));
    TxPrepareSendShardProgramsHgram = DataReqGroup->GetHistogram("TxPrepareSendShardProgramsUs",
        NMonitoring::ExponentialHistogram(10, 2, 100));

    MiniKQLResolveSentToShard = DataReqGroup->GetCounter("MiniKQLResolve/SentToShard", true);
    MiniKQLWrongRequest = DataReqGroup->GetCounter("MiniKQLResolve/WrongRequest", true);

    ReadTableResolveSentToShard = DataReqGroup->GetCounter("ReadTableResolve/SentToShard", true);
    ReadTableWrongRequest = DataReqGroup->GetCounter("ReadTableResolve/WrongRequest", true);

    MiniKQLProgramSize = DataReqGroup->GetCounter("MiniKQLProgramSize", true);
    MiniKQLParamsSize = DataReqGroup->GetCounter("MiniKQLParamsSize", true);

    ExecTimeout = DataReqGroup->GetCounter("ExecTimeout", true);

    MarkShardError = DataReqGroup->GetCounter("MarkShardError", true);

    PrepareErrorTimeout = DataReqGroup->GetCounter("PrepareErrorTimeout", true);

    PlanClientDestroyed = DataReqGroup->GetCounter("Plan/ClientDestroyed", true);
    PlanClientConnected = DataReqGroup->GetCounter("Plan/ClientConnected", true);
    PlanCoordinatorDeclined = DataReqGroup->GetCounter("Plan/CoordinatorDeclined", true);

    PlanClientTxResultComplete = DataReqGroup->GetCounter("Plan/TxResult/Complete", true);
    PlanClientTxResultAborted = DataReqGroup->GetCounter("Plan/TxResult/Aborted", true);
    PlanClientTxResultResultUnavailable = DataReqGroup->GetCounter("Plan/TxResult/ResultUnavailable", true);
    PlanClientTxResultCancelled = DataReqGroup->GetCounter("Plan/TxResult/Cancelled", true);
    PlanClientTxResultExecError = DataReqGroup->GetCounter("Plan/TxResult/ExecError", true);

    ClientTxStatusAccepted = DataReqGroup->GetCounter("ClientTx/Status/Accepted", true);
    ClientTxStatusProcessed = DataReqGroup->GetCounter("ClientTx/Status/Processed", true);
    ClientTxStatusConfirmed = DataReqGroup->GetCounter("ClientTx/Status/Confirmed", true);
    ClientTxStatusPlanned = DataReqGroup->GetCounter("ClientTx/Status/Planned", true);
    ClientTxStatusCoordinatorDeclined = DataReqGroup->GetCounter("ClientTx/Status/CoordinatorDeclined", true);

    MakeRequestProxyAccepted = DataReqGroup->GetCounter("MakeRequest/ProxyAccepted", true);
    MakeRequestWrongRequest = DataReqGroup->GetCounter("MakeRequest/WrongRequest", true);
    MakeRequestEmptyAffectedSet = DataReqGroup->GetCounter("MakeRequest/EmptyAffectedSet", true);

    ResolveKeySetLegacySuccess = DataReqGroup->GetCounter("ResolveKeySet/LegacySuccess", true);
    ResolveKeySetMiniKQLSuccess = DataReqGroup->GetCounter("ResolveKeySet/MiniKQLSuccess", true);
    ResolveKeySetReadTableSuccess = DataReqGroup->GetCounter("ResolveKeySet/ReadTableSuccess", true);
    ResolveKeySetRedirectUnavaible = DataReqGroup->GetCounter("ResolveKeySet/RedirectUnavaible", true);
    ResolveKeySetFail = DataReqGroup->GetCounter("ResolveKeySet/Fail", true);
    ResolveKeySetWrongRequest = DataReqGroup->GetCounter("ResolveKeySet/WrongRequest", true);
    ResolveKeySetDomainLocalityFail = DataReqGroup->GetCounter("ResolveKeySet/DomainLocalityFail", true);

    ClientConnectedOk = DataReqGroup->GetCounter("ClientConnected/Ok", true);
    ClientConnectedError = DataReqGroup->GetCounter("ClientConnected/Error", true);

    ClientDestroyedOk = DataReqGroup->GetCounter("ClientDestoroyed/Ok", true);
    ClientDestroyedError = DataReqGroup->GetCounter("ClientDestoroyed/Error", true);

    TxResultTabletPrepared = DataReqGroup->GetCounter("TxResult/TabletPrepared", true);
    TxResultPrepared = DataReqGroup->GetCounter("TxResult/Prepared", true);
    TxResultComplete = DataReqGroup->GetCounter("TxResult/Complete", true);
    TxResultError = DataReqGroup->GetCounter("TxResult/Error", true);
    TxResultAborted = DataReqGroup->GetCounter("TxResult/Aborted", true);
    TxResultFatal = DataReqGroup->GetCounter("TxResult/Fatal", true);
    TxResultShardOverloaded = DataReqGroup->GetCounter("TxResult/Overloaded", true);
    TxResultShardTryLater = DataReqGroup->GetCounter("TxResult/TryLater", true);
    TxResultExecError = DataReqGroup->GetCounter("TxResult/ExecError", true);
    TxResultResultUnavailable = DataReqGroup->GetCounter("TxResult/ResultUnavailable", true);
    TxResultCancelled = DataReqGroup->GetCounter("TxResult/Cancelled", true);

    MergeResultRequestExecError = DataReqGroup->GetCounter("MergeResult/RequestExecError", true);
    MergeResultRequestExecComplete = DataReqGroup->GetCounter("MergeResult/RequestExecComplete", true);
    MergeResultMiniKQLExecError = DataReqGroup->GetCounter("MergeResult/MiniKQLExecError", true);
    MergeResultMiniKQLExecComplete = DataReqGroup->GetCounter("MergeResult/MiniKQLExecComplete", true);
    MergeResultMiniKQLUnknownStatus = DataReqGroup->GetCounter("MergeResult/MiniKQLUnknownStatus", true);

    ResultsReceivedCount = DataReqGroup->GetCounter("ResultsReceived/Count", true);
    ResultsReceivedSize = DataReqGroup->GetCounter("ResultsReceived/Size", true);
}

}}
