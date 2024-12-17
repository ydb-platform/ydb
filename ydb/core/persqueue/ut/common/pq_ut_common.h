#pragma once

#include <ydb/core/persqueue/pq.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/user_info.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/protos/msgbus_kv.pb.h>


const bool ENABLE_DETAILED_PQ_LOG = false;
const bool ENABLE_DETAILED_KV_LOG = false;

namespace NKikimr::NPQ {

template <typename T>
inline constexpr static T PlainOrSoSlow(T plain, T slow) noexcept {
    return NSan::PlainOrUnderSanitizer(
        NValgrind::PlainOrUnderValgrind(plain, slow),
        slow
    );
}

constexpr ui32 NUM_WRITES = PlainOrSoSlow(50, 1);

void FillPQConfig(NKikimrPQ::TPQConfig& pqConfig, const TString& dbRoot, bool isFirstClass);

enum EventKing {
    TabletPipe,
    NPDisk,
    KeyValue,
    PQ
};

class TInitialEventsFilter : TNonCopyable {
    std::unordered_set<TString> Events;
public:
    TInitialEventsFilter() = default;

    TTestActorRuntime::TEventFilter Prepare(const std::unordered_set<EventKing>& eventKings = {TabletPipe, NPDisk, KeyValue, PQ},
                         const std::unordered_set<TString>& eventTypeNames = {}) {
        Events.clear();

        if (eventKings.contains(TabletPipe)) {
            Events.insert("NKikimr::TEvTabletPipe::TEvClientConnected");
            Events.insert("NKikimr::TEvTabletPipe::TEvClientDestroyed");
            Events.insert("NKikimr::TEvTabletPipe::TEvServerConnected");
        }
        if (eventKings.contains(NPDisk)) {
            Events.insert("NKikimr::NPDisk::TEvLog");
            Events.insert("NKikimr::NPDisk::TEvLogResult");
        }
        if (eventKings.contains(KeyValue)) {
            Events.insert("NKikimr::TEvKeyValue::TEvCollect");
            Events.insert("NKikimr::TEvKeyValue::TEvCompleteGC");
            Events.insert("NKikimr::TEvKeyValue::TEvIntermediate");
            Events.insert("NKikimr::TEvKeyValue::TEvPartialCompleteGC");
        }
        if (eventKings.contains(PQ)) {
            Events.insert("NKikimr::TEvPQ::TEvPartitionLabeledCounters");
            Events.insert("NKikimr::TEvPQ::TEvProxyResponse");
        }

        for(const auto& v : eventTypeNames) {
            Events.insert(v);
        }

        return [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
            return (*this)(runtime, event);
        };
    }

    bool operator()(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event) {
        Y_UNUSED(runtime);

        return Events.contains(event->GetTypeName());
    }
};

struct TTestContext {
    const TTabletTypes::EType PQTabletType = TTabletTypes::PersQueue;
    const TTabletTypes::EType BalancerTabletType = TTabletTypes::PersQueueReadBalancer;
    ui64 TabletId;
    ui64 BalancerTabletId;
    TInitialEventsFilter InitialEventsFilter;
    TVector<ui64> TabletIds;
    THolder<TTestActorRuntime> Runtime;
    TActorId Edge;
    THashMap<ui32, ui32> MsgSeqNoMap;
    bool EnableDetailedPQLog = ENABLE_DETAILED_PQ_LOG;

    TTestContext() {
        TabletId = MakeTabletID(false, 1);
        TabletIds.push_back(TabletId);

        BalancerTabletId = MakeTabletID(false, 2);
        TabletIds.push_back(BalancerTabletId);
    }

    static void SetupLogging(TTestActorRuntime& runtime, bool enableDetailedPQLog)  {
        NActors::NLog::EPriority pqPriority = enableDetailedPQLog ? NLog::PRI_DEBUG : NLog::PRI_INFO;
        NActors::NLog::EPriority priority = ENABLE_DETAILED_KV_LOG ? NLog::PRI_DEBUG : NLog::PRI_ERROR;
        NActors::NLog::EPriority otherPriority = NLog::PRI_INFO;

        runtime.SetLogPriority(NKikimrServices::PERSQUEUE, pqPriority);
        runtime.SetLogPriority(NKikimrServices::PERSQUEUE_READ_BALANCER, pqPriority);

        runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, pqPriority);
        runtime.SetLogPriority(NKikimrServices::KEYVALUE, priority);
        runtime.SetLogPriority(NKikimrServices::BOOTSTRAPPER, priority);
        runtime.SetLogPriority(NKikimrServices::TABLET_MAIN, priority);
        runtime.SetLogPriority(NKikimrServices::TABLET_EXECUTOR, priority);
        runtime.SetLogPriority(NKikimrServices::BS_PROXY, priority);

        runtime.SetLogPriority(NKikimrServices::HIVE, otherPriority);
        runtime.SetLogPriority(NKikimrServices::LOCAL, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_NODE, otherPriority);
        runtime.SetLogPriority(NKikimrServices::BS_CONTROLLER, otherPriority);
        runtime.SetLogPriority(NKikimrServices::TABLET_RESOLVER, otherPriority);

        runtime.SetLogPriority(NKikimrServices::PIPE_CLIENT, otherPriority);
        runtime.SetLogPriority(NKikimrServices::PIPE_SERVER, otherPriority);

        runtime.SetLogPriority(NKikimrServices::SYSTEM_VIEWS, otherPriority);
    }

    static bool RequestTimeoutFilter(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration duration, TInstant& deadline) {
        if (event->GetTypeRewrite() == TEvents::TSystem::Wakeup) {
            Cerr << "Captured TEvents::TSystem::Wakeup to " << runtime.FindActorName(event->GetRecipientRewrite()) << Endl;
            if (runtime.FindActorName(event->GetRecipientRewrite()) == "PERSQUEUE_ANS_ACTOR") {
                return true;
            }
        }

        if (event->GetTypeRewrite() == TEvPQ::EvUpdateAvailableSize) {
            deadline = runtime.GetTimeProvider()->Now() + duration;
            runtime.UpdateCurrentTime(deadline);
        }
        return false;
    }

    static bool ImmediateLogFlushAndRequestTimeoutFilter(TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event, TDuration duration, TInstant& deadline) {
        if (event->Type == NKikimr::TEvents::TEvFlushLog::EventType) {
            deadline = TInstant();
            return false;
        }

        deadline = runtime.GetTimeProvider()->Now() + duration;
        return RequestTimeoutFilter(runtime, event, duration, deadline);
    }

    void Prepare(const TString& dispatchName, std::function<void(TTestActorRuntime&)> setup, bool& outActiveZone, bool isFirstClass = false,
                 bool enableMonitoring = false, bool enableDbCounters = false) {
        Y_UNUSED(dispatchName);
        outActiveZone = false;
        TTestBasicRuntime* runtime = new TTestBasicRuntime();
        if (enableMonitoring) {
            runtime->SetupMonitoring();
        }

        Runtime.Reset(runtime);
        Runtime->SetScheduledLimit(200);

        TAppPrepare appData;
        appData.SetEnablePersistentQueryStats(enableDbCounters);
        appData.SetEnableDbCounters(enableDbCounters);
        SetupLogging(*Runtime, EnableDetailedPQLog);
        SetupTabletServices(*Runtime, &appData);
        setup(*Runtime);


        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(TabletId, PQTabletType, TErasureType::ErasureNone),
            &CreatePersQueue);

        FillPQConfig(Runtime->GetAppData(0).PQConfig, "/Root/PQ", isFirstClass);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);

        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(BalancerTabletId, BalancerTabletType, TErasureType::ErasureNone),
            &CreatePersQueueReadBalancer);

        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);

        Edge = Runtime->AllocateEdgeActor();

        Runtime->SetScheduledEventFilter(&RequestTimeoutFilter);

        outActiveZone = true;
    }

    void Prepare() {
        Runtime.Reset(new TTestBasicRuntime);
        Runtime->SetScheduledLimit(200);
        SetupLogging(*Runtime, EnableDetailedPQLog);
        SetupTabletServices(*Runtime);
        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(TabletId, PQTabletType, TErasureType::ErasureNone),
            &CreatePersQueue);

        Runtime->GetAppData(0).PQConfig.SetEnabled(true);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);

        CreateTestBootstrapper(*Runtime,
            CreateTestTabletInfo(BalancerTabletId, BalancerTabletType, TErasureType::ErasureNone),
            &CreatePersQueueReadBalancer);

        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);

        Edge = Runtime->AllocateEdgeActor();

        Runtime->SetScheduledEventFilter(&RequestTimeoutFilter);
    }


    void Finalize() {
        Runtime.Reset(nullptr);
    }
};

struct TFinalizer {
    TTestContext& TestContext;

    TFinalizer(TTestContext& testContext)
        : TestContext(testContext)
    {}

    ~TFinalizer() {
        TestContext.Finalize();
    }
};

/*
** SINGLE COMMAND TEST FUNCTIONS
*/

struct TTabletPreparationParameters {
    ui32 maxCountInPartition{20'000'000};
    ui64 maxSizeInPartition{100_MB};
    ui32 deleteTime{0}; // Delete instantly
    ui32 partitions{2};
    ui32 lowWatermark{6_MB};
    bool localDC{true};
    ui64 readFromTimestampsMs{0};
    ui64 sidMaxCount{0};
    ui32 specVersion{0};
    ui32 writeSpeed{0};
    i32 storageLimitBytes{0};
    TString folderId{"somefolder"};
    TString cloudId{"somecloud"};
    TString databaseId{"PQ"};
    TString databasePath{"/Root/PQ"};
    TString account{"federationAccount"};
    ::NKikimrPQ::TPQTabletConfig_EMeteringMode meteringMode = NKikimrPQ::TPQTabletConfig::METERING_MODE_RESERVED_CAPACITY;
};
void PQTabletPrepare(
    const TTabletPreparationParameters& parameters,
    const TVector<std::pair<TString, bool>>& users,
    TTestActorRuntime& runtime,
    ui64 tabletId,
    TActorId edge);

void PQBalancerPrepare(
    const TString topic,
    const TVector<std::pair<ui32, std::pair<ui64, ui32>>>& map,
    const ui64 ssId,
    TTestActorRuntime& runtime,
    ui64 tabletId,
    TActorId edge,
    const bool requireAuth = false,
    bool kill = true);

void PQTabletRestart(
    TTestActorRuntime& runtime,
    ui64 tabletId,
    TActorId edge);


/*
** TTestContext requiring functions
*/

void PQTabletPrepare(
    const TTabletPreparationParameters& parameters,
    const TVector<std::pair<TString, bool>>& users,
    TTestContext& context);

void PQBalancerPrepare(
    const TString topic,
    const TVector<std::pair<ui32, std::pair<ui64, ui32>>>& map,
    const ui64 ssId,
    TTestContext& context,
    const bool requireAuth = false,
    bool kill = true);

void PQTabletRestart(TTestContext& context);

TActorId RegisterReadSession(
   const TString& session,
   TTestContext& tc,
   const TVector<ui32>& groups = {});
void WaitReadSessionKill(TTestContext& tc);

TActorId SetOwner(
    const ui32 partition,
    TTestContext& tc,
    const TString& owner,
    bool force);

TActorId SetOwner(
    TTestActorRuntime* runtime,
    ui64 tabletId,
    const TActorId& sender,
    const ui32 partition,
    const TString& owner,
    bool force);

void FillDeprecatedUserInfo(
    NKikimrClient::TKeyValueRequest_TCmdWrite* write,
    const TString& client,
    ui32 partition,
    ui64 offset);

void FillUserInfo(
    NKikimrClient::TKeyValueRequest_TCmdWrite* write,
    const TString& client,
    ui32 partition,
    ui64 offset);

void PQGetPartInfo(
    ui64 startOffset,
    ui64 endOffset,
    TTestContext& tc);

void ReserveBytes(
    TTestContext& tc,
    const ui32 partition,
    const TString& cookie,
    i32 msgSeqNo,
    i64 size,
    const TActorId& pipeClient,
    bool lastRequest);

void WaitPartition(
    const TString &session,
    TTestContext& tc,
    ui32 partition,
    const TString& sessionToRelease,
    const TString& topic,
    const TActorId& pipe,
    bool ok = true);

void WriteData(
    const ui32 partition,
    const TString& sourceId,
    const TVector<std::pair<ui64, TString>> data,
    TTestContext& tc,
    const TString& cookie,
    i32 msgSeqNo,
    i64 offset,
    bool disableDeduplication = false);

void WriteData(
    TTestActorRuntime* runtime,
    ui64 tabletId,
    const TActorId& sender,
    const ui32 partition,
    const TString& sourceId,
    const TVector<std::pair<ui64, TString>> data,
    const TString& cookie,
    i32 msgSeqNo,
    i64 offset,
    bool disableDeduplication = false);

void WritePartData(
    const ui32 partition,
    const TString& sourceId,
    const i64 offset,
    const ui64 seqNo,
    const ui16 partNo,
    const ui16 totalParts,
    const ui32 totalSize,
    const TString& data,
    TTestContext& tc,
    const TString& cookie,
    i32 msgSeqNo);

void WritePartDataWithBigMsg(
    const ui32 partition,
    const TString& sourceId,
    const ui64 seqNo,
    const ui16 partNo,
    const ui16 totalParts,
    const ui32 totalSize,
    const TString& data,
    TTestContext& tc,
    const TString& cookie,
    i32 msgSeqNo,
    ui32 bigMsgSize);

//
// CMD's
//
TVector<TString> CmdSourceIdRead(TTestContext& tc);

struct TPQCmdSettingsBase {
    ui32 Partition = 0;
    TString User;
    TString Session;
    ui64 PartitionSessionId = 0;
    i64 Offset = 0;
    bool ToFail = false;
};

struct TPQCmdSettings : public TPQCmdSettingsBase {
    ui32 Generation = 0;
    ui32 Step = 0;
    bool KeepPipe = false;
    TPQCmdSettings() = default;
    TPQCmdSettings(ui32 partition, const TString& user, const TString& session, i64 offset = 0, ui32 generation = 0,
                   ui32 step = 0, bool error = false)
        : TPQCmdSettingsBase{partition, user, session, 0, offset, error}
        , Generation(generation)
        , Step(step)
    {}
};

struct TPQCmdReadSettings : public TPQCmdSettingsBase {
    ui32 Count = 0;
    ui32 Size = 0;
    ui32 ResCount = 0;
    bool Timeout = false;
    TVector<i32> Offsets;
    ui32 MaxTimeLagMs = 0;
    ui32 ReadTimestampMs = 0;
    ui64 DirectReadId = 0;
    TActorId Pipe;
    TPQCmdReadSettings() = default;
    TPQCmdReadSettings(const TString& session, ui32 partition, i64 offset, ui32 count, ui32 size, ui32 resCount, bool timeout = false,
                       TVector<i32> offsets = {}, const ui32 maxTimeLagMs = 0, const ui64 readTimestampMs = 0,
                       const TString user = "user")

        : TPQCmdSettingsBase{partition, user, session, 0, offset, false}
        , Count(count)
        , Size(size)
        , ResCount(resCount)
        , Timeout(timeout)
        , Offsets (offsets)
        , MaxTimeLagMs(maxTimeLagMs)
        , ReadTimestampMs(readTimestampMs)
    {}
};

struct TCmdDirectReadSettings {
    ui32 Partition;
    TString Session;
    ui64 PartitionSessionId;
    ui64 DirectReadId;
    TActorId Pipe;
    bool Fail = false;
};

std::pair<TString, TActorId> CmdSetOwner(
    const ui32 partition,
    TTestContext& tc,
    const TString& owner = "default",
    bool force = true);

std::pair<TString, TActorId> CmdSetOwner(
    TTestActorRuntime* runtime,
    ui64 tabletId,
    const TActorId& sender,
    const ui32 partition,
    const TString& owner = "default",
    bool force = true);

TActorId CmdCreateSession(const TPQCmdSettings& settings, TTestContext& tc);

void CmdGetOffset(
    const ui32 partition,
    const TString& user,
    i64 expectedOffset,
    TTestContext& tc,
    i64 ctime = -1,
    ui64 writeTime = 0);

void CmdKillSession(
    const ui32 partition,
    const TString& user,
    const TString& session,
    TTestContext& tc,
    const TActorId& pipe = {});

void CmdRead(
    const ui32 partition,
    const ui64 offset,
    const ui32 count,
    const ui32 size,
    const ui32 resCount,
    bool timeouted,
    TTestContext& tc,
    TVector<i32> offsets = {},
    const ui32 maxTimeLagMs = 0,
    const ui64 readTimestampMs = 0,
    const TString user = "user");

void CmdRead(
    const TPQCmdReadSettings& settings,
    TTestContext& tc);

void CmdPublishRead(const TCmdDirectReadSettings& settings, TTestContext& tc);
void CmdForgetRead(const TCmdDirectReadSettings& settings, TTestContext& tc);

void CmdReserveBytes(
    const ui32 partition,
    TTestContext& tc,
    const TString& ownerCookie,
    i32 msn, i64 size,
    TActorId pipeClient,
    bool noAnswer = false,
    bool lastRequest = false);

void CmdSetOffset(
    const ui32 partition,
    const TString& user,
    ui64 offset,
    bool error,
    TTestContext& tc,
    const TString& session = "");

void CmdUpdateWriteTimestamp(
    const ui32 partition,
    ui64 timestamp,
    TTestContext& tc);

void CmdWrite(
    const ui32 partition,
    const TString& sourceId,
    const TVector<std::pair<ui64, TString>> data,
    TTestContext& tc,
    bool error = false,
    const THashSet<ui32>& alreadyWrittenSeqNo = {},
    bool isFirst = false,
    const TString& ownerCookie = "",
    i32 msn = -1,
    i64 offset = -1,
    bool treatWrongCookieAsError = false,
    bool treatBadOffsetAsError = true,
    bool disableDeduplication = false);

void CmdWrite(
    TTestActorRuntime* runtime,
    ui64 tabletId,
    const TActorId& sender,
    const ui32 partition,
    const TString& sourceId,
    ui32& msgSeqNo,
    const TVector<std::pair<ui64, TString>> data,
    bool error = false,
    const THashSet<ui32>& alreadyWrittenSeqNo = {},
    bool isFirst = false,
    const TString& ownerCookie = "",
    i32 msn = -1,
    i64 offset = -1,
    bool treatWrongCookieAsError = false,
    bool treatBadOffsetAsError = true,
    bool disableDeduplication = false);

THolder<TEvPersQueue::TEvPeriodicTopicStats> GetReadBalancerPeriodicTopicStats(TTestActorRuntime& runtime, ui64 balancerId);

} // namespace NKikimr::NPQ
