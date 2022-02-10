#pragma once

#include "defs.h"
#include <ydb/core/protos/tablet_tracing_signals.pb.h>
#include <ydb/core/tracing/signal.h>
#include <ydb/core/base/tracing.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_types.h>
#include <util/generic/set.h>
#include <ydb/core/protos/tablet.pb.h>

namespace NKikimr {
namespace NTracing {

    namespace NSignalTypes {
        enum ESignalType : ITraceSignal::EType {
            TypeOnTabletBootstrap = ITraceSignal::ES_TABLET_BOOTSTRAP,
            TypeOnHandleStateStorageInfoResolve,
            TypeOnCancelTablet,
            TypeOnLockedInitializationPath,
            TypeOnHandleStateStorageInfoLock,
            TypeOnPromoteToCandidate,
            TypeOnTabletBlockBlobStorage,
            TypeOnTabletRebuildGraph,
            TypeOnWriteZeroEntry,
            TypeOnFollowerPromoteToLeader,
            TypeRebuildGraphBootstrap,
            TypeErrorEntryBeyondBlocked,
            TypeErrorUnknownStatus,
            TypeErrorParsingFromString,
            TypeErrorSendRefsCheck,
            TypeErrorRebuildGraph,
            TypeOnProcessKeyEntry,
            TypeOnProcessZeroEntry,
            TypeOnProcessLogEntry,
            TypeOnDiscoverRangeRequest,
            TypeOnApplyDiscoveryRange,
            TypeOnMakeHistory,
            TypeOnCheckRefsGetResult,
            TypeOnBuildHistoryGraph,
            TypeOnRebuildGraphResult
        };
    }

    class TOnTabletBootstrap : public TTraceSignal<TOnTabletBootstrap,
        NKikimrTracing::TOnTabletBootstrap,
        NSignalTypes::TypeOnTabletBootstrap> {
    public:
        TOnTabletBootstrap(ui32 suggestedGeneration, bool leader, const TActorId& proxyID);
        TOnTabletBootstrap(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnHandleStateStorageInfoResolve :
        public TTraceSignal<TOnHandleStateStorageInfoResolve,
        NKikimrTracing::TOnHandleStateStorageInfoResolve,
        NSignalTypes::TypeOnHandleStateStorageInfoResolve> {
    public:
        TOnHandleStateStorageInfoResolve(ui32 knownGeneration, ui32 knownStep, ui32 signatureSz);
        TOnHandleStateStorageInfoResolve(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnCancelTablet : public TTraceSignal<TOnCancelTablet,
        NKikimrTracing::TOnCancelTablet,
        NSignalTypes::TypeOnCancelTablet> {
    public:
        TOnCancelTablet(ui64 tabletID
            , TTabletTypes::EType tabletType
            , ui32 tabletDeadReason
            , ui32 suggestedGeneration
            , ui32 knownGeneration
        );
        TOnCancelTablet(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnLockedInitializationPath : public TTraceSignal<TOnLockedInitializationPath,
        NKikimrTracing::TOnLockedInitializationPath,
        NSignalTypes::TypeOnLockedInitializationPath> {
    public:
        TOnLockedInitializationPath(ui32 knownGeneration, ui32 knownStep, ui32 signatureSz);
        TOnLockedInitializationPath(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnHandleStateStorageInfoLock : public TTraceSignal<TOnHandleStateStorageInfoLock,
        NKikimrTracing::TOnHandleStateStorageInfoLock,
        NSignalTypes::TypeOnHandleStateStorageInfoLock> {
    public:
        TOnHandleStateStorageInfoLock(ui32 knownGeneration, ui32 knownStep, ui32 signatureSz);
        TOnHandleStateStorageInfoLock(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnPromoteToCandidate : public TTraceSignal<TOnPromoteToCandidate,
        NKikimrTracing::TOnPromoteToCandidate,
        NSignalTypes::TypeOnPromoteToCandidate> {
    public:
        TOnPromoteToCandidate(ui32 knownGeneration);
        TOnPromoteToCandidate(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnTabletBlockBlobStorage : public TTraceSignal<TOnTabletBlockBlobStorage,
        NKikimrTracing::TOnTabletBlockBlobStorage,
        NSignalTypes::TypeOnTabletBlockBlobStorage> {
    public:
        TOnTabletBlockBlobStorage(const TActorId& reqBlockBlobStorageID, ui32 knownGeneration);
        TOnTabletBlockBlobStorage(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnTabletRebuildGraph : public TTraceSignal<TOnTabletRebuildGraph,
        NKikimrTracing::TOnTabletRebuildGraph,
        NSignalTypes::TypeOnTabletRebuildGraph> {
    public:
        TOnTabletRebuildGraph(const TActorId& tabletReqRebuildGraphID, const TTraceID& rebuildGraphTraceID);
        TOnTabletRebuildGraph(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnWriteZeroEntry : public TTraceSignal<TOnWriteZeroEntry,
        NKikimrTracing::TOnWriteZeroEntry,
        NSignalTypes::TypeOnWriteZeroEntry> {
    public:
        TOnWriteZeroEntry(
            const std::pair<ui32, ui32>& snapshot
            , ui32 lastGeneration
            , ui32 confirmedStep
            , ui32 lastInGeneration
        );
        TOnWriteZeroEntry(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnFollowerPromoteToLeader : public TTraceSignal<TOnFollowerPromoteToLeader,
        NKikimrTracing::TOnFollowerPromoteToLeader,
        NSignalTypes::TypeOnFollowerPromoteToLeader> {
    public:
        TOnFollowerPromoteToLeader(
            ui32 suggestedGeneration
            , const TActorId& knownLeaderID
            , const TActorId& followerStStGuardian
        );
        TOnFollowerPromoteToLeader(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TRebuildGraphBootstrap : public TTraceSignal<TRebuildGraphBootstrap,
        NKikimrTracing::TRebuildGraphBootstrap,
        NSignalTypes::TypeRebuildGraphBootstrap> {
    public:
        TRebuildGraphBootstrap(ui32 blockedGen);
        TRebuildGraphBootstrap(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TErrorEntryBeyondBlocked : public TTraceSignal<TErrorEntryBeyondBlocked,
        NKikimrTracing::TErrorEntryBeyondBlocked,
        NSignalTypes::TypeErrorEntryBeyondBlocked> {
    public:
        TErrorEntryBeyondBlocked(const TLogoBlobID& latest, ui32 blockedGen);
        TErrorEntryBeyondBlocked(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TErrorUnknownStatus : public TTraceSignal<TErrorUnknownStatus,
        NKikimrTracing::TErrorUnknownStatus,
        NSignalTypes::TypeErrorUnknownStatus> {
    public:
        TErrorUnknownStatus(NKikimrProto::EReplyStatus status, const TString& reason);
        TErrorUnknownStatus(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TErrorParsingFromString : public TTraceSignal<TErrorParsingFromString,
        NKikimrTracing::TErrorParsingFromString,
        NSignalTypes::TypeErrorParsingFromString> {
    public:
        TErrorParsingFromString(const TLogoBlobID& blobID);
        TErrorParsingFromString(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TErrorSendRefsCheck : public TTraceSignal<TErrorSendRefsCheck,
        NKikimrTracing::TErrorSendRefsCheck,
        NSignalTypes::TypeErrorSendRefsCheck> {
    public:
        TErrorSendRefsCheck();
        TErrorSendRefsCheck(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TErrorRebuildGraph : public TTraceSignal<TErrorRebuildGraph,
        NKikimrTracing::TErrorRebuildGraph,
        NSignalTypes::TypeErrorRebuildGraph> {
    public:
        TErrorRebuildGraph(ui32 generation, ui32 step);
        TErrorRebuildGraph(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnProcessKeyEntry : public TTraceSignal<TOnProcessKeyEntry,
        NKikimrTracing::TOnProcessKeyEntry,
        NSignalTypes::TypeOnProcessKeyEntry> {
    public:
        TOnProcessKeyEntry(const TLogoBlobID& latestEntry);
        TOnProcessKeyEntry(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnProcessZeroEntry : public TTraceSignal<TOnProcessZeroEntry,
        NKikimrTracing::TOnProcessZeroEntry,
        NSignalTypes::TypeOnProcessZeroEntry> {
    public:
        TOnProcessZeroEntry(ui32 generation, const std::pair<ui32, ui32>& snapshot, const std::pair<ui32, ui32>& confirmed);
        TOnProcessZeroEntry(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnProcessLogEntry : public TTraceSignal<TOnProcessLogEntry,
        NKikimrTracing::TOnProcessLogEntry,
        NSignalTypes::TypeOnProcessLogEntry> {
    public:
        TOnProcessLogEntry(const TLogoBlobID& blobID
            , const std::pair<ui32, ui32>& snapshot
            , const std::pair<ui32, ui32>& confirmed
            , NKikimrTabletBase::TTabletLogEntry& logEntry
        );
        TOnProcessLogEntry(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
        void OutHtmlHeader(TStringStream& str, TTimestampData& tsData, std::function<TString()> getMyId) const override;
    };

    class TOnDiscoverRangeRequest : public TTraceSignal<TOnDiscoverRangeRequest,
        NKikimrTracing::TOnDiscoverRangeRequest,
        NSignalTypes::TypeOnDiscoverRangeRequest> {
    public:
        TOnDiscoverRangeRequest(ui32 group, const TLogoBlobID& from, const TLogoBlobID& to);
        TOnDiscoverRangeRequest(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnApplyDiscoveryRange : public TTraceSignal<TOnApplyDiscoveryRange,
        NKikimrTracing::TOnApplyDiscoveryRange,
        NSignalTypes::TypeOnApplyDiscoveryRange> {
    public:
        TOnApplyDiscoveryRange(ui32 group, const TLogoBlobID& from, const TLogoBlobID& to);
        TOnApplyDiscoveryRange(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnMakeHistory : public TTraceSignal<TOnMakeHistory,
        NKikimrTracing::TOnMakeHistory,
        NSignalTypes::TypeOnMakeHistory> {
    public:
        TOnMakeHistory(TSet<TLogoBlobID>& refsToCheck);
        TOnMakeHistory(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
        void OutHtmlHeader(TStringStream& str, TTimestampData& tsData, std::function<TString()> getMyId) const override;
    };

    class TOnCheckRefsGetResult : public TTraceSignal<TOnCheckRefsGetResult,
        NKikimrTracing::TOnCheckRefsGetResult,
        NSignalTypes::TypeOnCheckRefsGetResult> {
    public:
        TOnCheckRefsGetResult(ui32 responseSz);
        TOnCheckRefsGetResult(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
    };

    class TOnBuildHistoryGraph : public TTraceSignal<TOnBuildHistoryGraph,
        NKikimrTracing::TOnBuildHistoryGraph,
        NSignalTypes::TypeOnBuildHistoryGraph> {
    public:
        TOnBuildHistoryGraph(TEvTablet::TDependencyGraph* graph);
        TOnBuildHistoryGraph(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
        void OutHtmlHeader(TStringStream& str, TTimestampData& tsData, std::function<TString()> getMyId) const override;
    };

    class TOnRebuildGraphResult : public TTraceSignal<TOnRebuildGraphResult,
        NKikimrTracing::TOnRebuildGraphResult,
        NSignalTypes::TypeOnRebuildGraphResult> {
    public:
        TOnRebuildGraphResult(NTracing::ITrace *trace);
        TOnRebuildGraphResult(const TString& serializedSignal);
        void OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const override;
        void OutHtmlHeader(TStringStream& str, TTimestampData& tsData, std::function<TString()> getMyId) const override;
        void OutHtmlBody(TStringStream& str
            , const TTimestampInfo& tsInfo
            , std::function<TString()> getMyId
            , TList<ui64>& signalAddress
        ) const override;
    };

}
}
