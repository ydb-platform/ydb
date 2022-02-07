#include "tablet_tracing_signals.h"
#include <ydb/core/tracing/http.h>
#include <ydb/core/base/tablet.h>

namespace NKikimr {
namespace NTracing {

template <typename TStreamImpl>
void PrintGenStepPair(TStreamImpl& str, ui64 genStepInt) {
    auto genStepPair = ExpandGenStepPair(genStepInt);
    str << genStepPair.first << ":" << genStepPair.second;
}

template <typename TStreamImpl>
void PrintLogoBlob(TStreamImpl& str, const NKikimrProto::TLogoBlobID& protoLogoBlobId) {
    TLogoBlobID logoBlobId = LogoBlobIDFromLogoBlobID(protoLogoBlobId);
    str << logoBlobId.ToString();
}

TSignalFactory& GetSignalFactoryInstance() {
    static TSignalFactory SingleFactory;
    return SingleFactory;
}

struct TSignalTypeRegistrator {
    TSignalTypeRegistrator() {
        Initialize();
    }

    void Initialize() {
        Register<TOnTabletBootstrap>(NSignalTypes::TypeOnTabletBootstrap);
        Register<TOnHandleStateStorageInfoResolve>(NSignalTypes::TypeOnHandleStateStorageInfoResolve);
        Register<TOnCancelTablet>(NSignalTypes::TypeOnCancelTablet);
        Register<TOnLockedInitializationPath>(NSignalTypes::TypeOnLockedInitializationPath);
        Register<TOnHandleStateStorageInfoLock>(NSignalTypes::TypeOnHandleStateStorageInfoLock);
        Register<TOnPromoteToCandidate>(NSignalTypes::TypeOnPromoteToCandidate);
        Register<TOnTabletBlockBlobStorage>(NSignalTypes::TypeOnTabletBlockBlobStorage);
        Register<TOnTabletRebuildGraph>(NSignalTypes::TypeOnTabletRebuildGraph);
        Register<TOnWriteZeroEntry>(NSignalTypes::TypeOnWriteZeroEntry);
        Register<TOnFollowerPromoteToLeader>(NSignalTypes::TypeOnFollowerPromoteToLeader);
        Register<TRebuildGraphBootstrap>(NSignalTypes::TypeRebuildGraphBootstrap);
        Register<TErrorEntryBeyondBlocked>(NSignalTypes::TypeErrorEntryBeyondBlocked);
        Register<TErrorUnknownStatus>(NSignalTypes::TypeErrorUnknownStatus);
        Register<TErrorParsingFromString>(NSignalTypes::TypeErrorParsingFromString);
        Register<TErrorSendRefsCheck>(NSignalTypes::TypeErrorSendRefsCheck);
        Register<TErrorRebuildGraph>(NSignalTypes::TypeErrorRebuildGraph);
        Register<TOnProcessKeyEntry>(NSignalTypes::TypeOnProcessKeyEntry);
        Register<TOnProcessZeroEntry>(NSignalTypes::TypeOnProcessZeroEntry);
        Register<TOnProcessLogEntry>(NSignalTypes::TypeOnProcessLogEntry);
        Register<TOnDiscoverRangeRequest>(NSignalTypes::TypeOnDiscoverRangeRequest);
        Register<TOnApplyDiscoveryRange>(NSignalTypes::TypeOnApplyDiscoveryRange);
        Register<TOnMakeHistory>(NSignalTypes::TypeOnMakeHistory);
        Register<TOnCheckRefsGetResult>(NSignalTypes::TypeOnCheckRefsGetResult);
        Register<TOnBuildHistoryGraph>(NSignalTypes::TypeOnBuildHistoryGraph);
        Register<TOnRebuildGraphResult>(NSignalTypes::TypeOnRebuildGraphResult);
    }

private:
    template <typename TDerived>
    void Register(ITraceSignal::EType signalType) {
        TSignalFactory::Instance().Add<TDerived>(signalType);
    }
};

// Singleton
static TSignalTypeRegistrator SignalTypeRegistrator;

TOnTabletBootstrap::TOnTabletBootstrap(ui32 suggestedGeneration, bool leader, const TActorId& proxyID) {
    PbSignal.SetSuggestedGeneration(suggestedGeneration);
    PbSignal.SetLeader(leader);
    NActors::ActorIdToProto(proxyID, PbSignal.MutableStateStorageProxyID());
}

TOnTabletBootstrap::TOnTabletBootstrap(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnTabletBootstrap::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    TActorId ID = NActors::ActorIdFromProto(PbSignal.GetStateStorageProxyID());
    str << prefix << TimeStamp(tsData) <<  " Bootstrapped. Sent TEvLookup to SS proxy " << ID.ToString() << Endl;
}

TOnHandleStateStorageInfoResolve::TOnHandleStateStorageInfoResolve(
    ui32 knownGeneration
    , ui32 knownStep
    , ui32 signatureSz
) {
    PbSignal.SetKnownGeneration(knownGeneration);
    PbSignal.SetKnownStep(knownStep);
    PbSignal.SetSignatureSz(signatureSz);
}

TOnHandleStateStorageInfoResolve::TOnHandleStateStorageInfoResolve(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnHandleStateStorageInfoResolve::OutText(
    TStringStream& str
    , TTimestampData& tsData
    , const TString& prefix
) const {
    str << prefix << TimeStamp(tsData)
        << " Received TEvStateStorage::TEvInfo (KnownGeneration=" << PbSignal.GetKnownGeneration()
        << ", KnownStep=" << PbSignal.GetKnownStep()
        << ", SignatureSz=" << PbSignal.GetSignatureSz()
        << ")" << Endl;
}

TOnCancelTablet::TOnCancelTablet(ui64 tabletID
    , TTabletTypes::EType tabletType
    , ui32 tabletDeadReason
    , ui32 suggestedGeneration
    , ui32 knownGeneration
) {
    PbSignal.SetTabletID(tabletID);
    PbSignal.SetTabletType(tabletType);
    PbSignal.SetTabletDeadReason(tabletDeadReason);
    PbSignal.SetSuggestedGeneration(suggestedGeneration);
    PbSignal.SetKnownGeneration(knownGeneration);
}

TOnCancelTablet::TOnCancelTablet(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnCancelTablet::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " CancelTablet called (TabletID=" << PbSignal.GetTabletID()
        << ", TabletType=" << TTabletTypes::TypeToStr((TTabletTypes::EType)PbSignal.GetTabletType())
        << ", DeadReason="
        << TEvTablet::TEvTabletDead::Str((TEvTablet::TEvTabletDead::EReason)PbSignal.GetTabletDeadReason())
        << ", SuggestedGeneration=" << PbSignal.GetSuggestedGeneration()
        << ")" << Endl;
}

TOnLockedInitializationPath::TOnLockedInitializationPath(ui32 knownGeneration, ui32 knownStep, ui32 signatureSz) {
    PbSignal.SetKnownGeneration(knownGeneration);
    PbSignal.SetKnownStep(knownStep);
    PbSignal.SetSignatureSz(signatureSz);
}

TOnLockedInitializationPath::TOnLockedInitializationPath(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnLockedInitializationPath::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " LockedInitializationPath (KnownGeneration=" << PbSignal.GetKnownGeneration()
        << ", KnownStep=" << PbSignal.GetKnownStep()
        << ", SignatureSz=" << PbSignal.GetSignatureSz()
        << ")" << Endl;
}

TOnHandleStateStorageInfoLock::TOnHandleStateStorageInfoLock(
    ui32 knownGeneration
    , ui32 knownStep
    , ui32 signatureSz
) {
    PbSignal.SetKnownGeneration(knownGeneration);
    PbSignal.SetKnownStep(knownStep);
    PbSignal.SetSignatureSz(signatureSz);
}

TOnHandleStateStorageInfoLock::TOnHandleStateStorageInfoLock(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnHandleStateStorageInfoLock::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData)
        << " Successfully locked the storage (KnownGeneration=" << PbSignal.GetKnownGeneration()
        << ", KnownStep=" << PbSignal.GetKnownStep()
        << ", SignatureSz=" << PbSignal.GetSignatureSz()
        << ")" << Endl;
}

TOnPromoteToCandidate::TOnPromoteToCandidate(ui32 knownGeneration) {
    PbSignal.SetKnownGeneration(knownGeneration);
}

TOnPromoteToCandidate::TOnPromoteToCandidate(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnPromoteToCandidate::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData)
        << " Promoted to candidate (KnownGeneration=" << PbSignal.GetKnownGeneration() << ")" << Endl;
}

TOnTabletBlockBlobStorage::TOnTabletBlockBlobStorage(const TActorId& reqBlockBlobStorageID, ui32 knownGeneration) {
    NActors::ActorIdToProto(reqBlockBlobStorageID, PbSignal.MutableReqBlockBlobStorageID());
    PbSignal.SetKnownGeneration(knownGeneration);
}

TOnTabletBlockBlobStorage::TOnTabletBlockBlobStorage(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnTabletBlockBlobStorage::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    TActorId ID = NActors::ActorIdFromProto(PbSignal.GetReqBlockBlobStorageID());
    str << prefix << TimeStamp(tsData) << " Created ReqBlockBlobStorage Actor (" << ID.ToString()
        << ") with KnownGeneration=" << PbSignal.GetKnownGeneration() << Endl;
}

TOnTabletRebuildGraph::TOnTabletRebuildGraph(
    const TActorId& tabletReqRebuildGraphID
    , const TTraceID& rebuildGraphTraceID
) {
    NActors::ActorIdToProto(tabletReqRebuildGraphID, PbSignal.MutableTabletReqRebuildGraphID());
    TraceIDFromTraceID(rebuildGraphTraceID, PbSignal.MutableRebuildGraphTraceID());
}

TOnTabletRebuildGraph::TOnTabletRebuildGraph(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnTabletRebuildGraph::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    TActorId ID = NActors::ActorIdFromProto(PbSignal.GetTabletReqRebuildGraphID());
    str << prefix << TimeStamp(tsData) << " Created ReqRebuildHistoryGraph Actor (" << ID.ToString() << ")" << Endl;
}

TOnWriteZeroEntry::TOnWriteZeroEntry(
    const std::pair<ui32, ui32>& snapshot
    , ui32 lastGeneration
    , ui32 confirmedStep
    , ui32 lastInGeneration
) {
    PbSignal.SetSnapshot(MakeGenStepPair(snapshot.first, snapshot.second));
    PbSignal.SetZeroConfirmed(MakeGenStepPair(lastGeneration, confirmedStep));
    PbSignal.SetLastInGeneration(lastInGeneration);
}

TOnWriteZeroEntry::TOnWriteZeroEntry(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnWriteZeroEntry::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " WriteZeroEntry (Snapshot=";
    PrintGenStepPair(str, PbSignal.GetSnapshot());
    str << ", ZeroConfirmed=";
    PrintGenStepPair(str, PbSignal.GetZeroConfirmed());
    str << ", LastInGeneration=" << PbSignal.GetLastInGeneration() << ")" << Endl;
}

TOnFollowerPromoteToLeader::TOnFollowerPromoteToLeader(
    ui32 suggestedGeneration
    , const TActorId& knownLeaderID
    , const TActorId& followerStStGuardian
) {
    PbSignal.SetSuggestedGeneration(suggestedGeneration);
    NActors::ActorIdToProto(knownLeaderID, PbSignal.MutableKnownLeaderID());
    NActors::ActorIdToProto(followerStStGuardian, PbSignal.MutableFollowerStStGuardian());
}

TOnFollowerPromoteToLeader::TOnFollowerPromoteToLeader(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnFollowerPromoteToLeader::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    TActorId knownLeaderId = NActors::ActorIdFromProto(PbSignal.GetKnownLeaderID());
    TActorId followerStStGuardianId = NActors::ActorIdFromProto(PbSignal.GetFollowerStStGuardian());
    str << prefix << TimeStamp(tsData)
        << " Follower promoted to leader (SuggestedGeneration=" << PbSignal.GetSuggestedGeneration()
        << ", KnownLeaderID=" << knownLeaderId.ToString()
        << ", FollowerStStGuardian=" << followerStStGuardianId.ToString() << ")" << Endl;
}

TRebuildGraphBootstrap::TRebuildGraphBootstrap(ui32 blockedGen) {
    PbSignal.SetBlockedGen(blockedGen);
}

TRebuildGraphBootstrap::TRebuildGraphBootstrap(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TRebuildGraphBootstrap::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData)
        << " ReqRebuildHistoryGraph actor bootstrapped. BlockedGen=" << PbSignal.GetBlockedGen() << Endl;
}

TErrorEntryBeyondBlocked::TErrorEntryBeyondBlocked(const TLogoBlobID& latest, ui32 blockedGen) {
    LogoBlobIDFromLogoBlobID(latest, PbSignal.MutableLatest());
    PbSignal.SetBlockedGen(blockedGen);
}

TErrorEntryBeyondBlocked::TErrorEntryBeyondBlocked(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TErrorEntryBeyondBlocked::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " Found entry beyond blocked generation. LastBlobID=";
    PrintLogoBlob(str, PbSignal.GetLatest());
    str << ", BlockedGen=" << PbSignal.GetBlockedGen() << Endl;
}

TErrorUnknownStatus::TErrorUnknownStatus(NKikimrProto::EReplyStatus status, const TString& reason) {
    PbSignal.SetStatus(status);
    PbSignal.SetReason(reason);
}

TErrorUnknownStatus::TErrorUnknownStatus(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TErrorUnknownStatus::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData)
        << " Handle TEvFindLatestLogEntryResult: Status=" << NKikimrProto::EReplyStatus_Name(PbSignal.GetStatus())
        << ", Reason=\"" << PbSignal.GetReason() << "\"" << Endl;
}

TErrorParsingFromString::TErrorParsingFromString(const TLogoBlobID& blobID) {
    LogoBlobIDFromLogoBlobID(blobID, PbSignal.MutableBlobID());
}

TErrorParsingFromString::TErrorParsingFromString(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TErrorParsingFromString::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " logBody ParseFromString error for BlobID=";
    PrintLogoBlob(str, PbSignal.GetBlobID());
    str << Endl;
}

TErrorSendRefsCheck::TErrorSendRefsCheck() {
}

TErrorSendRefsCheck::TErrorSendRefsCheck(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TErrorSendRefsCheck::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " SendRefsCheck error" << Endl;
}

TErrorRebuildGraph::TErrorRebuildGraph(ui32 generation, ui32 step) {
    PbSignal.SetGeneration(generation);
    PbSignal.SetStep(step);
}

TErrorRebuildGraph::TErrorRebuildGraph(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TErrorRebuildGraph::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " Graph rebuild error - no Log entry for " << PbSignal.GetGeneration()
        << ":" << PbSignal.GetStep() << Endl;
}

TOnProcessKeyEntry::TOnProcessKeyEntry(const TLogoBlobID& latestEntry) {
    LogoBlobIDFromLogoBlobID(latestEntry, PbSignal.MutableLatestEntry());
}

TOnProcessKeyEntry::TOnProcessKeyEntry(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnProcessKeyEntry::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " Last entry found: ";
    PrintLogoBlob(str, PbSignal.GetLatestEntry());
    str << Endl;
}

TOnProcessZeroEntry::TOnProcessZeroEntry(
    ui32 generation
    , const std::pair<ui32, ui32>& snapshot
    , const std::pair<ui32, ui32>& confirmed
) {
    PbSignal.SetGeneration(generation);
    PbSignal.SetSnapshot(MakeGenStepPair(snapshot.first, snapshot.second));
    PbSignal.SetConfirmed(MakeGenStepPair(confirmed.first, confirmed.second));
}

TOnProcessZeroEntry::TOnProcessZeroEntry(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnProcessZeroEntry::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData)
        << " ProcessZeroEntry (Generation=" << PbSignal.GetGeneration() << ", Snapshot=";
    PrintGenStepPair(str, PbSignal.GetSnapshot());
    str << ", Confirmed=";
    PrintGenStepPair(str, PbSignal.GetConfirmed());
    str << ")" << Endl;
}

TOnProcessLogEntry::TOnProcessLogEntry(
    const TLogoBlobID& blobID
    , const std::pair<ui32, ui32>& snapshot
    , const std::pair<ui32, ui32>& confirmed
    , NKikimrTabletBase::TTabletLogEntry& logEntry
) {
    LogoBlobIDFromLogoBlobID(blobID, PbSignal.MutableBlobID());
    PbSignal.SetSnapshot(MakeGenStepPair(snapshot.first, snapshot.second));
    PbSignal.SetConfirmed(MakeGenStepPair(confirmed.first, confirmed.second));
    *PbSignal.MutableReferences() = logEntry.GetReferences();
    *PbSignal.MutableGcDiscovered() = logEntry.GetGcDiscovered();
    *PbSignal.MutableGcLeft() = logEntry.GetGcLeft();
}

TOnProcessLogEntry::TOnProcessLogEntry(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnProcessLogEntry::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " ProcessLogEntry (ID=";
    PrintLogoBlob(str, PbSignal.GetBlobID());
    str << ", Snapshot=";
    PrintGenStepPair(str, PbSignal.GetSnapshot());
    str << ", Confirmed=";
    PrintGenStepPair(str, PbSignal.GetConfirmed());
    str << ")" << Endl;

    size_t refSize = PbSignal.ReferencesSize();
    str << prefix << "  " << refSize << " References";
    if (refSize) {
        str << ":" << Endl;
        for (size_t i = 0; i < refSize; ++i) {
            str << prefix << "    ";
            PrintLogoBlob(str, PbSignal.references(i));
            str << Endl;
        }
    } else {
        str << Endl;
    }

    size_t gcDiscoveredSize = PbSignal.GcDiscoveredSize();
    str << prefix << "  " << gcDiscoveredSize << " GcDiscovered";
    if (gcDiscoveredSize) {
        str << ":" << Endl;
        for (size_t i = 0; i < gcDiscoveredSize; ++i) {
            str << prefix << "    ";
            PrintLogoBlob(str, PbSignal.gcdiscovered(i));
            str << Endl;
        }
    } else {
        str << Endl;
    }

    size_t gcLeftSize = PbSignal.GcLeftSize();
    str << prefix << "  " << gcLeftSize << " GcLeft";
    if (gcLeftSize) {
        str << ":" << Endl;
        for (size_t i = 0; i < gcLeftSize; ++i) {
            str << prefix << "    ";
            PrintLogoBlob(str, PbSignal.gcleft(i));
            str << Endl;
        }
    } else {
        str << Endl;
    }
}

void TOnProcessLogEntry::OutHtmlHeader(
    TStringStream& str
    , TTimestampData& tsData
    , std::function<TString()> getMyId
) const {
    HTML(str) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {
                str << TimeStamp(tsData) << " ";
                TString myId = getMyId();
                TStringBuilder textHeader;
                textHeader << "ProcessLogEntry (ID=";
                PrintLogoBlob(textHeader, PbSignal.GetBlobID());
                textHeader << ", Snapshot=";
                PrintGenStepPair(textHeader, PbSignal.GetSnapshot());
                textHeader << ", Confirmed=";
                PrintGenStepPair(textHeader, PbSignal.GetConfirmed());
                textHeader << ")";
                COLLAPSED_REF_CONTENT(myId, textHeader) {
                    DIV_CLASS("tab-left") {
                        size_t refSize = PbSignal.ReferencesSize();
                        DIV_CLASS("row") {
                            DIV_CLASS("col-md-12") {
                                str << refSize << " References:";
                            }
                        }
                        DIV_CLASS("tab-left") {
                            for (size_t i = 0; i < refSize; ++i) {
                                DIV_CLASS("row") {
                                    DIV_CLASS("col-md-12") {
                                        PrintLogoBlob(str, PbSignal.references(i));
                                    }
                                }
                            }
                        }

                        size_t gcDiscoveredSize = PbSignal.GcDiscoveredSize();
                        DIV_CLASS("row") {
                            DIV_CLASS("col-md-12") {
                                str << gcDiscoveredSize << " GcDiscovered:";
                            }
                        }
                        DIV_CLASS("tab-left") {
                            for (size_t i = 0; i < gcDiscoveredSize; ++i) {
                                DIV_CLASS("row") {
                                    DIV_CLASS("col-md-12") {
                                        PrintLogoBlob(str, PbSignal.gcdiscovered(i));
                                    }
                                }
                            }
                        }

                        size_t gcLeftSize = PbSignal.GcLeftSize();
                        DIV_CLASS("row") {
                            DIV_CLASS("col-md-12") {
                                str << gcLeftSize << " GcLeft:";
                            }
                        }
                        DIV_CLASS("tab-left") {
                            for (size_t i = 0; i < gcLeftSize; ++i) {
                                DIV_CLASS("row") {
                                    DIV_CLASS("col-md-12") {
                                        PrintLogoBlob(str, PbSignal.gcleft(i));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

TOnDiscoverRangeRequest::TOnDiscoverRangeRequest(ui32 group, const TLogoBlobID& from, const TLogoBlobID& to) {
    PbSignal.SetGroup(group);
    LogoBlobIDFromLogoBlobID(from, PbSignal.MutableFrom());
    LogoBlobIDFromLogoBlobID(to, PbSignal.MutableTo());
}

TOnDiscoverRangeRequest::TOnDiscoverRangeRequest(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnDiscoverRangeRequest::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " DiscoverRangeRequest sent (Group=" << PbSignal.GetGroup() << ", From=";
    PrintLogoBlob(str, PbSignal.GetFrom());
    str << ", To=";
    PrintLogoBlob(str, PbSignal.GetTo());
    str << ")" << Endl;
}

TOnApplyDiscoveryRange::TOnApplyDiscoveryRange(ui32 group, const TLogoBlobID& from, const TLogoBlobID& to) {
    PbSignal.SetGroup(group);
    LogoBlobIDFromLogoBlobID(from, PbSignal.MutableFrom());
    LogoBlobIDFromLogoBlobID(to, PbSignal.MutableTo());
}

TOnApplyDiscoveryRange::TOnApplyDiscoveryRange(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnApplyDiscoveryRange::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " ApplyDiscoveryRange (Group=" << PbSignal.GetGroup() << ", From=";
    PrintLogoBlob(str, PbSignal.GetFrom());
    str << ", To=";
    PrintLogoBlob(str, PbSignal.GetTo());
    str << ")" << Endl;
}

TOnMakeHistory::TOnMakeHistory(TSet<TLogoBlobID>& refsToCheck) {
    LogoBlobIDRepatedFromLogoBlobIDUniversal(PbSignal.MutableRefsToCheck(), refsToCheck);
}

TOnMakeHistory::TOnMakeHistory(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnMakeHistory::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " OnMakeHistory(" << PbSignal.RefsToCheckSize() << " entries)" << Endl;
    for (size_t i = 0; i < PbSignal.RefsToCheckSize(); ++i) {
        str << prefix << "  ";
        PrintLogoBlob(str, PbSignal.refstocheck(i));
        str << Endl;
    }
}

void TOnMakeHistory::OutHtmlHeader(TStringStream& str, TTimestampData& tsData, std::function<TString()> getMyId) const {
    HTML(str) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {
                str << TimeStamp(tsData) << " ";
                TString myId = getMyId();
                TString textHeader = " OnMakeHistory(" + ToString(PbSignal.RefsToCheckSize()) + " entries)";
                COLLAPSED_REF_CONTENT(myId, textHeader) {
                    DIV_CLASS("tab-left") {
                        for (size_t i = 0; i < PbSignal.RefsToCheckSize(); ++i) {
                            DIV_CLASS("row") {
                                DIV_CLASS("col-md-12") {
                                    PrintLogoBlob(str, PbSignal.refstocheck(i));
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

TOnCheckRefsGetResult::TOnCheckRefsGetResult(ui32 responseSz) {
    PbSignal.SetResponseSz(responseSz);
}

TOnCheckRefsGetResult::TOnCheckRefsGetResult(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnCheckRefsGetResult::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " CheckReferences(" << PbSignal.GetResponseSz() << " refs)" << Endl;
}

TOnBuildHistoryGraph::TOnBuildHistoryGraph(TEvTablet::TDependencyGraph* graph) {
    if (!graph) {
        return;
    }
    for (const auto& graphEntry : graph->Entries) {
        auto& pbEntry = *PbSignal.AddEntries();
        pbEntry.SetGeneration(graphEntry.Id.first);
        pbEntry.SetStep(graphEntry.Id.second);
        pbEntry.SetIsSnapshot(graphEntry.IsSnapshot);
        LogoBlobIDRepatedFromLogoBlobIDVector(pbEntry.MutableReferences()
            , graphEntry.References.begin()
            , graphEntry.References.end());
        LogoBlobIDRepatedFromLogoBlobIDVector(pbEntry.MutableGcDiscovered()
            , graphEntry.GcDiscovered.begin()
            , graphEntry.GcDiscovered.end());
        LogoBlobIDRepatedFromLogoBlobIDVector(pbEntry.MutableGcLeft()
            , graphEntry.GcLeft.begin()
            , graphEntry.GcLeft.end());
    }
}

TOnBuildHistoryGraph::TOnBuildHistoryGraph(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void PrintBuildHistoryGraphEntry(TStringStream& str
    , const NKikimrTracing::TOnBuildHistoryGraph::TEntry& entry
    , const TString& prefix) {
    size_t refSize = entry.ReferencesSize();
    str << prefix << "{" << Endl
        << prefix << "  " << entry.GetGeneration() << ":" << entry.GetStep()
        << ", IsSnapshot=" << entry.GetIsSnapshot() << Endl
        << prefix << "  " << refSize << " References";
    if (refSize) {
        str << ":" << Endl;
        for (size_t i = 0; i < refSize; ++i) {
            str << prefix << "    ";
            PrintLogoBlob(str, entry.references(i));
            str << Endl;
        }
    } else {
        str << Endl;
    }
    size_t gcDiscoveredSize = entry.GcDiscoveredSize();
    str << prefix << "  " << gcDiscoveredSize << " GcDiscovered";
    if (gcDiscoveredSize) {
        str << ":" << Endl;
        for (size_t i = 0; i < gcDiscoveredSize; ++i) {
            str << prefix << "    ";
            PrintLogoBlob(str, entry.gcdiscovered(i));
            str << Endl;
        }
    } else {
        str << Endl;
    }
    size_t gcLeftSize = entry.GcLeftSize();
    str << prefix << "  " << gcLeftSize << " GcLeft";
    if (gcLeftSize) {
        str << ":" << Endl;
        for (size_t i = 0; i < gcLeftSize; ++i) {
            str << prefix << "    ";
            PrintLogoBlob(str, entry.gcleft(i));
            str << Endl;
        }
    } else {
        str << Endl;
    }
    str << prefix << "}" << Endl;
}

void PrintBuildHistoryGraphEntryHtml(TStringStream& str
    , const NKikimrTracing::TOnBuildHistoryGraph::TEntry& entry
    , std::function<TString()> getMyId
) {
    HTML(str) {
        TString myId = getMyId();
        TStringStream textHeader;
        textHeader << entry.GetGeneration() << ":" << entry.GetStep()
            << " (IsSnapshot = " << entry.GetIsSnapshot() << ")";
        COLLAPSED_REF_CONTENT(myId, textHeader.Str()) {
            DIV_CLASS("tab-left") {
                size_t refSize = entry.ReferencesSize();
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        str << refSize << " References:";
                    }
                }
                DIV_CLASS("tab-left") {
                    for (size_t i = 0; i < refSize; ++i) {
                        DIV_CLASS("row") {
                            DIV_CLASS("col-md-12") {
                                PrintLogoBlob(str, entry.references(i));
                            }
                        }
                    }
                }
                size_t gcDiscoveredSize = entry.GcDiscoveredSize();
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        str << gcDiscoveredSize << " GcDiscovered:";
                    }
                }
                DIV_CLASS("tab-left") {
                    for (size_t i = 0; i < gcDiscoveredSize; ++i) {
                        DIV_CLASS("row") {
                            DIV_CLASS("col-md-12") {
                                PrintLogoBlob(str, entry.gcdiscovered(i));
                            }
                        }
                    }
                }
                size_t gcLeftSize = entry.GcLeftSize();
                DIV_CLASS("row") {
                    DIV_CLASS("col-md-12") {
                        str << gcLeftSize << " GcLeft:";
                    }
                }
                DIV_CLASS("tab-left") {
                    for (size_t i = 0; i < gcLeftSize; ++i) {
                        DIV_CLASS("row") {
                            DIV_CLASS("col-md-12") {
                                PrintLogoBlob(str, entry.gcleft(i));
                            }
                        }
                    }
                }
            }
        }
    }
}

void TOnBuildHistoryGraph::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " History graph built(" << PbSignal.EntriesSize() << " entries)" << Endl;
    for (size_t i = 0; i < PbSignal.EntriesSize(); ++i) {
        PrintBuildHistoryGraphEntry(str, PbSignal.entries(i), prefix);
    }
}

void TOnBuildHistoryGraph::OutHtmlHeader(
    TStringStream& str
    , TTimestampData& tsData
    , std::function<TString()> getMyId
) const {
    HTML(str) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {
                str << TimeStamp(tsData) << " ";
                TString myId = getMyId();
                TString textHeader = " History graph built(" + ToString(PbSignal.EntriesSize()) + " entries)";
                COLLAPSED_REF_CONTENT(myId, textHeader) {
                    DIV_CLASS("tab-left") {
                        for (size_t i = 0; i < PbSignal.EntriesSize(); ++i) {
                            DIV_CLASS("row") {
                                DIV_CLASS("col-md-12") {
                                    PrintBuildHistoryGraphEntryHtml(
                                        str,
                                        PbSignal.entries(i),
                                        [i, getMyId](){ return getMyId() + ToString(i); }
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

TOnRebuildGraphResult::TOnRebuildGraphResult(ITrace* trace) {
    TString str;
    if (trace->SerializeToString(str)) {
        PbSignal.SetSerializedTrace(str);
    }
}

TOnRebuildGraphResult::TOnRebuildGraphResult(const TString& serializedSignal)
    : TMyBase(serializedSignal)
{}

void TOnRebuildGraphResult::OutText(TStringStream& str, TTimestampData& tsData, const TString& prefix) const {
    str << prefix << TimeStamp(tsData) << " RebuildGraph result received. Here's the trace:" << Endl;
    str << prefix << "{" << Endl;
    THolder<ITrace> trace(CreateTrace(PbSignal.GetSerializedTrace()));
    trace->OutText(str, tsData, "    ");
    str << prefix << "}" << Endl;
}

void TOnRebuildGraphResult::OutHtmlHeader(
    TStringStream& str
    , TTimestampData& tsData
    , std::function<TString()> getMyId
) const {
    HTML(str) {
        DIV_CLASS("row") {
            DIV_CLASS("col-md-12") {
                str << TimeStamp(tsData) << " ";
                TString myId = getMyId();
                COLLAPSED_REF_CONTENT_AJAX(myId, tsData, "RebuildGraph result received") {
                    DIV_CLASS("tab-left") {
                        str << "Loading data...";
                    }
                }
            }
        }
    }
}

void TOnRebuildGraphResult::OutHtmlBody(
    TStringStream& str,
    const TTimestampInfo& tsInfo,
    std::function<TString()> getMyId,
    TList<ui64>& signalAddress
) const {
    Y_UNUSED(getMyId);
    Y_UNUSED(signalAddress);
    THolder<ITrace> trace(CreateTrace(PbSignal.GetSerializedTrace()));
    if (signalAddress.size()) {
        // cuttent signal is not the target
        trace->OutSignalHtmlBody(str, tsInfo, getMyId, signalAddress);
    } else {
        // current signal is the target
        trace->OutHtml(str, tsInfo, getMyId);
    }
}

}
}
