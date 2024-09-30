#include "msgbus_server_persqueue.h"
#include "msgbus_server_pq_read_session_info.h"

#include <ydb/core/base/tabletid.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/keyvalue/keyvalue_events.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pq.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/fake_scheme_shard.h>
#include <ydb/core/testlib/mock_pq_metacache.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/is_in.h>
#include <util/string/join.h>
#include <util/system/type_name.h>

#include <list>
#include <typeinfo>

namespace NKikimr {
namespace NMsgBusProxy {

using namespace testing;
using namespace NSchemeCache;

void FillValidTopicRequest(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request, ui64 topicsCount);
void MakeEmptyTopic(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request);
void MakeDuplicatedTopic(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request);
void MakeDuplicatedPartition(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request);

const static TString topic1 = "rt3.dc1--topic1";
const static TString topic2 = "rt3.dc1--topic2";
// Base test class with useful helpers for constructing all you need to test pq requests.
class TMessageBusServerPersQueueRequestTestBase: public TTestBase {
protected:
    void SetUp() override {
        TTestBase::SetUp();

        //TTestActorRuntime::SetVerbose(true); // debug events

        // Initialize runtime
        Runtime = MakeHolder<TTestBasicRuntime>();
        Runtime->SetObserverFunc([this](TAutoPtr<IEventHandle>& event) {
            return EventsObserver(event);
        });
        Runtime->SetRegistrationObserverFunc([this](TTestActorRuntimeBase& runtime, const TActorId& parentId, const TActorId& actorId) {
            TTestActorRuntime::DefaultRegistrationObserver(runtime, parentId, actorId);
            return RegistrationObserver(parentId, actorId);
        });
        SetupTabletServices(*Runtime); // Calls Runtime->Initialize();

        // Edge actor
        EdgeActorId = Runtime->AllocateEdgeActor();

        // Logging
        Runtime->SetLogPriority(NKikimrServices::PERSQUEUE, NLog::PRI_DEBUG);
        Runtime->GetAppData(0).PQConfig.SetEnabled(true);

        // NOTE(shmel1k@): KIKIMR-14221
        Runtime->GetAppData(0).PQConfig.SetTopicsAreFirstClassCitizen(false);
        Runtime->GetAppData(0).PQConfig.SetRequireCredentialsInNewProtocol(false);
        Runtime->GetAppData(0).PQConfig.SetClusterTablePath("/Root/PQ/Config/V2/Cluster");
        Runtime->GetAppData(0).PQConfig.SetVersionTablePath("/Root/PQ/Config/V2/Versions");
        Runtime->GetAppData(0).PQConfig.SetRoot("/Root/PQ");
    }

    void TearDown() override {
        // Assertions
        try {
            AssertTestActorsDestroyed();
            UNIT_ASSERT(TestMainActorHasAnswered);
        } catch (...) {
            // If assertions will throw, we need to clear all resources
        }

        // Cleanup
        Runtime.Reset();
        MockPQMetaCache = nullptr;
        Actor = nullptr;
        TestMainActorHasAnswered = false;
        EdgeActorId = TActorId();
        TestMainActorId = TActorId();
        EdgeEventHandle.Reset();
        LoadedFakeSchemeShard = false;
        TestActors.clear();
        PausedEventTypes.clear();
        PausedEvents.clear();
        TTestBase::TearDown();
    }

    //
    // Helpers
    //

    TMockPQMetaCache& GetMockPQMetaCache() {
        if (!MockPQMetaCache) {
            MockPQMetaCache = new TMockPQMetaCache();
            Runtime->Register(MockPQMetaCache);
        }
        return *MockPQMetaCache;
    }

    void EnsureHasFakeSchemeShard() {
        if (!LoadedFakeSchemeShard) {
            TFakeSchemeShardState::TPtr state{new TFakeSchemeShardState()};
            BootFakeSchemeShard(*Runtime, 123, state);
            LoadedFakeSchemeShard = true;
        }
    }

    THolder<TEvPersQueue::TEvUpdateBalancerConfig> MakeUpdateBalancerConfigRequest(const TString& topic, const TVector<std::pair<ui32, ui64>>& partitionsToTablets, const ui64 schemeShardId = 123) {
        static int version = 0;
        ++version;

        THolder<TEvPersQueue::TEvUpdateBalancerConfig> request = MakeHolder<TEvPersQueue::TEvUpdateBalancerConfig>();
        for (const auto& p : partitionsToTablets) {
            auto* part = request->Record.AddPartitions();
            part->SetPartition(p.first);
            part->SetTabletId(p.second);
        }
        request->Record.SetTxId(12345);
        request->Record.SetPathId(1);
        request->Record.SetVersion(version);
        request->Record.SetTopicName(topic);
        request->Record.SetPath("path");
        request->Record.SetSchemeShardId(schemeShardId);
        return request;
    }

    TActorId StartBalancer(ui64 balancerTabletId) {
        TActorId id = CreateTestBootstrapper(*Runtime,
                                       CreateTestTabletInfo(balancerTabletId, TTabletTypes::PersQueueReadBalancer, TErasureType::ErasureNone),
                                       &CreatePersQueueReadBalancer);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);
        return id;
    }

    TActorId PrepareBalancer(const TString& topic, ui64 balancerTabletId, const TVector<std::pair<ui32, ui64>>& partitionsToTablets, const ui64 schemeShardId = 123) {
        EnsureHasFakeSchemeShard();
        TActorId id = StartBalancer(balancerTabletId);

        THolder<TEvPersQueue::TEvUpdateBalancerConfig> request = MakeUpdateBalancerConfigRequest(topic, partitionsToTablets, schemeShardId);

        Runtime->SendToPipe(balancerTabletId, EdgeActorId, request.Release(), 0, GetPipeConfigWithRetries());
        TAutoPtr<IEventHandle> handle;
        TEvPersQueue::TEvUpdateConfigResponse* result = Runtime->GrabEdgeEvent<TEvPersQueue::TEvUpdateConfigResponse>(handle);

        UNIT_ASSERT(result != nullptr);
        const auto& rec = result->Record;
        UNIT_ASSERT(rec.HasStatus() && rec.GetStatus() == NKikimrPQ::OK);
        UNIT_ASSERT(rec.HasTxId() && rec.GetTxId() == 12345);
        UNIT_ASSERT(rec.HasOrigin() && result->GetOrigin() == balancerTabletId);

        ForwardToTablet(*Runtime, balancerTabletId, EdgeActorId, new TEvents::TEvPoisonPill());
        TDispatchOptions rebootOptions;
        rebootOptions.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvRestored, 2));
        Runtime->DispatchEvents(rebootOptions);
        return id;
    }

    THolder<TEvPersQueue::TEvUpdateConfig> MakeUpdatePQRequest(const TString& topic, const TVector<size_t>& partitions) {
        static int version = 0;
        ++version;

        auto request = MakeHolder<TEvPersQueue::TEvUpdateConfigBuilder>();
        for (size_t i : partitions) {
            request->Record.MutableTabletConfig()->AddPartitionIds(i);
        }
        request->Record.MutableTabletConfig()->SetCacheSize(10*1024*1024);
        request->Record.SetTxId(12345);
        auto tabletConfig = request->Record.MutableTabletConfig();
        tabletConfig->SetTopicName(topic);
        tabletConfig->SetVersion(version);
        auto config = tabletConfig->MutablePartitionConfig();
        config->SetMaxCountInPartition(20000000);
        config->SetMaxSizeInPartition(100 * 1024 * 1024);
        config->SetLifetimeSeconds(0);
        config->SetSourceIdLifetimeSeconds(1*60*60);
        config->SetMaxWriteInflightSize(90000000);
        config->SetLowWatermark(6*1024*1024);

        return request;
    }

    TActorId StartPQTablet(ui64 tabletId) {
        TActorId id = CreateTestBootstrapper(*Runtime,
                                       CreateTestTabletInfo(tabletId, TTabletTypes::PersQueue, TErasureType::ErasureNone),
                                       &CreatePersQueue);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
        Runtime->DispatchEvents(options);
        return id;
    }

    TActorId PreparePQTablet(const TString& topic, ui64 tabletId, const TVector<size_t>& partitions) {
        EnsureHasFakeSchemeShard();
        TActorId id = StartPQTablet(tabletId);

        TAutoPtr<IEventHandle> handle;
        {
            THolder<TEvPersQueue::TEvUpdateConfig> request = MakeUpdatePQRequest(topic, partitions);
            Runtime->SendToPipe(tabletId, EdgeActorId, request.Release(), 0, GetPipeConfigWithRetries());
            TEvPersQueue::TEvUpdateConfigResponse* result = Runtime->GrabEdgeEvent<TEvPersQueue::TEvUpdateConfigResponse>(handle);

            UNIT_ASSERT(result);
            auto& rec = result->Record;
            UNIT_ASSERT_C(rec.HasStatus() && rec.GetStatus() == NKikimrPQ::OK, "rec: " << rec);
            UNIT_ASSERT_C(rec.HasTxId() && rec.GetTxId() == 12345, "rec: " << rec);
            UNIT_ASSERT_C(rec.HasOrigin() && result->GetOrigin() == tabletId, "rec: " << rec);
        }

        {
            THolder<TEvKeyValue::TEvRequest> request;
            request.Reset(new TEvKeyValue::TEvRequest);
            auto read = request->Record.AddCmdRead();
            read->SetKey("_config");

            Runtime->SendToPipe(tabletId, EdgeActorId, request.Release(), 0, GetPipeConfigWithRetries());
            TEvKeyValue::TEvResponse* result = Runtime->GrabEdgeEvent<TEvKeyValue::TEvResponse>(handle);

            UNIT_ASSERT(result);
            UNIT_ASSERT(result->Record.HasStatus());
            UNIT_ASSERT_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
        }
        return id;
    }

    // Register tested actor
    void RegisterActor(const NKikimrClient::TPersQueueRequest& request) {
        Actor = CreateActorServerPersQueue(
            EdgeActorId,
            request,
            GetMockPQMetaCache().SelfId()
        );
        TestMainActorId = Runtime->Register(Actor);
        TestActors.insert(TestMainActorId);
    }

    TEvPersQueue::TEvResponse* GrabResponseEvent() {
        TEvPersQueue::TEvResponse* responseEvent = Runtime->GrabEdgeEvent<TEvPersQueue::TEvResponse>(EdgeEventHandle);
        UNIT_ASSERT(responseEvent);
        TestMainActorHasAnswered = true;
        return responseEvent;
    }

    const TEvPersQueue::TEvResponse* GetResponse() {
        UNIT_ASSERT(EdgeEventHandle.Get() != nullptr);
        return EdgeEventHandle->Get<TEvPersQueue::TEvResponse>();
    }

    size_t ResponseFieldsCount() {
        const TEvPersQueue::TEvResponse* resp = GetResponse();
        UNIT_ASSERT(resp != nullptr);
        const auto& r = resp->Record;
        const auto& m = r.GetMetaResponse();
        const auto& p = r.GetPartitionResponse();
        return r.HasFetchResponse()
            + m.HasCmdGetPartitionOffsetsResult()
            + m.HasCmdGetTopicMetadataResult()
            + m.HasCmdGetPartitionLocationsResult()
            + m.HasCmdGetPartitionStatusResult()
            + m.HasCmdGetReadSessionsInfoResult()
            + (p.CmdWriteResultSize() > 0)
            + p.HasCmdGetMaxSeqNoResult()
            + p.HasCmdReadResult()
            + p.HasCmdGetClientOffsetResult()
            + p.HasCmdGetOwnershipResult();
    }


    template<class T>
    bool AssertTopicResponsesImpl(const T& t, const TString& topic, NPersQueue::NErrorCode::EErrorCode code, ui32 numParts) {
        if (t.GetTopic() != topic) return false;
        UNIT_ASSERT_C(t.GetErrorCode() == code, "for topic " << topic << " code is " << (ui32) t.GetErrorCode() << " but waiting for " << (ui32) code << " resp: " << t);
        UNIT_ASSERT_C(t.PartitionResultSize() == numParts, "for topic " << topic << " parts size  is not " << numParts << " resp: " << t);
        return true;
    }

    template<class T>
    bool AssertTopicResponsesImpl(const T& t, const TString& topic, NPersQueue::NErrorCode::EErrorCode code) {
        if (t.GetTopic() != topic) return false;
        UNIT_ASSERT_C(t.GetErrorCode() == code, "for topic " << topic << " code is " << (ui32) t.GetErrorCode() << " but waiting for " << (ui32) code << " resp: " << t);
        return true;
    }


    void AssertTopicResponses(const TString& topic, NPersQueue::NErrorCode::EErrorCode code, ui32 numParts) {
        const TEvPersQueue::TEvResponse* resp = GetResponse();
        UNIT_ASSERT(resp != nullptr);
        for (auto& r : resp->Record.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult()) {
            if (AssertTopicResponsesImpl(r, topic, code, numParts))
                return;
        }
        for (auto& r : resp->Record.GetMetaResponse().GetCmdGetPartitionLocationsResult().GetTopicResult()) {
            if (r.GetTopic() != topic) continue;
            UNIT_ASSERT_C(r.GetErrorCode() == code, "for topic " << topic << " code is " << (ui32) r.GetErrorCode() << " but waiting for " << (ui32) code << " resp: " << r);
            UNIT_ASSERT_C(r.PartitionLocationSize() == numParts, "for topic " << topic << " parts size  is not " << numParts << " resp: " << r);
            return;
        }
        for (auto& r : resp->Record.GetMetaResponse().GetCmdGetPartitionStatusResult().GetTopicResult()) {
            if (AssertTopicResponsesImpl(r, topic, code, numParts))
                return;
        }
        for (auto& r : resp->Record.GetMetaResponse().GetCmdGetPartitionOffsetsResult().GetTopicResult()) {
            if (AssertTopicResponsesImpl(r, topic, code, numParts))
                return;
        }
        for (auto& r : resp->Record.GetMetaResponse().GetCmdGetTopicMetadataResult().GetTopicInfo()) {
            if (AssertTopicResponsesImpl(r, topic, code))
                return;
        }
        UNIT_ASSERT_C(false, "topic " << topic << " not found in response " << resp->Record);
    }


    void AssertFailedResponse(NPersQueue::NErrorCode::EErrorCode code, const THashSet<TString>& markers = {}, EResponseStatus status = MSTATUS_ERROR) {
        const TEvPersQueue::TEvResponse* resp = GetResponse();
        Cerr << "Assert failed: Check response: " << resp->Record << Endl;
        UNIT_ASSERT(resp != nullptr);
        UNIT_ASSERT_C(resp->Record.HasStatus(), "Response: " << resp->Record);
        UNIT_ASSERT_UNEQUAL_C(resp->Record.GetStatus(), 1, "Response: " << resp->Record);
        UNIT_ASSERT_EQUAL_C(resp->Record.GetErrorCode(), code, "code: "  << (ui32)code << " Response: " << resp->Record);
        UNIT_ASSERT_C(!resp->Record.GetErrorReason().empty(), "Response: " << resp->Record);
        UNIT_ASSERT_VALUES_EQUAL_C(resp->Record.GetStatus(), status, "Response: " << resp->Record);
        if (!markers.empty()) {
            const TString reason = resp->Record.GetErrorReason();
            UNIT_ASSERT_STRING_CONTAINS_C(reason, "Marker# ", reason << " doesn't contain any marker, but it should.");
            const size_t markerPos = reason.find("Marker# ");
            UNIT_ASSERT_UNEQUAL(markerPos, TString::npos);
            const TString marker = reason.substr(markerPos);
            UNIT_ASSERT_C(IsIn(markers, marker), marker << " is not in the specified set: {" << JoinSeq(", ", markers) << "}");
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ResponseFieldsCount(), 0, "Response: " << resp->Record);
    }

    void AssertFailedResponse(NPersQueue::NErrorCode::EErrorCode code, const char* marker, EResponseStatus status = MSTATUS_ERROR) {
        AssertFailedResponse(code, THashSet<TString>({marker}), status);
    }

    void AssertSucceededResponse() {
        const TEvPersQueue::TEvResponse* resp = GetResponse();
        UNIT_ASSERT(resp != nullptr);
        UNIT_ASSERT_VALUES_EQUAL_C(resp->Record.GetStatus(), 1, "Response: " << resp->Record);
        UNIT_ASSERT_C(resp->Record.HasErrorCode(), "Response: " << resp->Record);
        UNIT_ASSERT_EQUAL_C(resp->Record.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
        UNIT_ASSERT_C(resp->Record.GetErrorReason().empty(), "Response: " << resp->Record);
        UNIT_ASSERT_VALUES_EQUAL_C(ResponseFieldsCount(), 1, "Response: " << resp->Record);
    }

    TTestActorRuntime::EEventAction EventsObserver(TAutoPtr<IEventHandle>& event) {
        switch (event->Type) {
        case NKikimr::TEvPersQueue::EvResponse:
            {
                if (event->Sender == TestMainActorId) {
                    TestMainActorHasAnswered = true;
                    UNIT_ASSERT_EQUAL(event->Recipient, EdgeActorId);
                }
                break;
            }
        }
        if ((IsIn(PausedEventTypes, event->Type) || IsIn(PausedEventTypes, 0)) && IsIn(TestActors, event->Recipient)) {
            PausedEvents.push_back(event);
            return TTestActorRuntime::EEventAction::DROP;
        }
        return TTestActorRuntime::EEventAction::PROCESS;
    }

    void RegistrationObserver(const TActorId& parentId, const TActorId& actorId) {
        if (IsIn(TestActors, parentId)) {
            IActor* child = Runtime->FindActor(actorId);
            UNIT_ASSERT(child);
            UNIT_ASSERT(TestActors.insert(actorId).second);
        }
    }

    // TODO: move this code to test actor runtime
    void AssertTestActorsDestroyed() {
        auto events = Runtime->CaptureEvents();
        THashSet<TActorId> destroyedActors;
        for (const auto& event : events) {
            if (event->Type == TEvents::TSystem::PoisonPill) {
                destroyedActors.insert(event->Recipient);
            }
        }
        for (const TActorId& actorId : TestActors) {
            IActor* actor = Runtime->FindActor(actorId);
            if (actor != nullptr) {
                const bool isPipe = actor->ActivityType == NKikimrServices::TActivity::TABLET_PIPE_CLIENT;
                if (isPipe) {
                    UNIT_ASSERT_C(IsIn(destroyedActors, actorId),
                                  "Pipe client was not destroyed after test actor worked. Pipe client actor id: " << actorId);
                } else {
                    UNIT_ASSERT_C(IsIn(destroyedActors, actorId),
                                  "Test actor or its child wasn't destroyed. Actor id: " << actorId
                                  << ". Type: " << TypeName(*actor));
                }
            }
        }
    }

    // TODO: move this code to test actor runtime
    void PauseInputForTestActors(ui64 eventType = 0) {
        PausedEventTypes.insert(eventType);
    }

    template <class TEvent>
    void PauseInputForTestActors() {
        PauseInputForTestActors(TEvent::EventType);
    }

    void ResumeEventsForTestActors() {
        PausedEventTypes.clear();
        for (TAutoPtr<IEventHandle>& event : PausedEvents) {
            if (event.Get() != nullptr) {
                Runtime->Send(event.Release());
            }
        }
        PausedEvents.clear();
    }

protected:
    TActorId EdgeActorId;
    IActor* Actor = nullptr;
    TActorId TestMainActorId;
    bool TestMainActorHasAnswered = false;
    TMockPQMetaCache* MockPQMetaCache = nullptr;
    TAutoPtr<IEventHandle> EdgeEventHandle;
    bool LoadedFakeSchemeShard = false;
    THashSet<TActorId> TestActors; // Actor and its children
    THashSet<ui64> PausedEventTypes;
    std::list<TAutoPtr<IEventHandle>> PausedEvents;
    THolder<TTestActorRuntime> Runtime;

};

// Common tests that are actual to all pq requests.
// To run these tests you need to insert COMMON_TESTS_LIST() macro in particular command's test.
class TMessageBusServerPersQueueRequestCommonTest: public TMessageBusServerPersQueueRequestTestBase {
public:
#define COMMON_TESTS_LIST()                                                     \
    UNIT_TEST(HandlesTimeout)                                                   \
    UNIT_TEST(FailsOnFailedGetAllTopicsRequest)                                 \
    UNIT_TEST(FailsOnBadRootStatusInGetNodeRequest)                             \
    UNIT_TEST(FailesOnNotATopic)                                                \
    UNIT_TEST(FailsOnNotOkStatusInGetNodeRequest)                               \
    UNIT_TEST(FailsOnNoBalancerInGetNodeRequest)                                \
    UNIT_TEST(FailsOnZeroBalancerTabletIdInGetNodeRequest)                      \
    UNIT_TEST(FailsOnBalancerDescribeResultFailureWhenTopicsAreGivenExplicitly) \
    /**/

    virtual NKikimrClient::TPersQueueRequest MakeValidRequest(ui64 topicsCount = 2) = 0;

    void SetBalancerId(TSchemeCacheNavigate::TResultSet& resultSet, ui64 index, const TMaybe<ui64>& tabletId) {
        auto* newInfo = new TSchemeCacheNavigate::TPQGroupInfo(*resultSet[index].PQGroupInfo);
        if (tabletId.Defined()) {
            newInfo->Description.SetBalancerTabletID(*tabletId);
        } else {
            newInfo->Description.ClearBalancerTabletID();
        }
        resultSet[index].PQGroupInfo.Reset(newInfo);
    }

    TSchemeCacheNavigate::TEntry MakeEntry(
            ui64 topicId,
            TSchemeCacheNavigate::EStatus status = TSchemeCacheNavigate::EStatus::Ok,
            TSchemeCacheNavigate::EKind kind = TSchemeCacheNavigate::KindTopic,
            bool makePQDescription = true
    ) {
        TSchemeCacheNavigate::TEntry entry;
        entry.Status = status;
        entry.Kind = kind;
        entry.Path = {"Root", "PQ"};
        if (status != TSchemeCacheNavigate::EStatus::Ok || kind != TSchemeCacheNavigate::KindTopic
                                                        || !makePQDescription
        ) {
            return entry;
        }
        auto *pqInfo = new TSchemeCacheNavigate::TPQGroupInfo();
        pqInfo->Kind = TSchemeCacheNavigate::KindTopic;
        auto &descr = pqInfo->Description;
        switch (topicId) {
            case 1:
                descr.SetName(topic1);
                descr.SetBalancerTabletID(MakeTabletID(false, 100));
                descr.SetAlterVersion(42);
                descr.SetPartitionPerTablet(1);
                break;
            case 2:
                descr.SetName(topic2);
                descr.SetBalancerTabletID(MakeTabletID(false, 200));
                descr.SetAlterVersion(5);
                descr.SetPartitionPerTablet(3);
                break;
            default:
                UNIT_FAIL("");
        }
        auto* pqTabletConfig = descr.MutablePQTabletConfig();
        pqTabletConfig->SetTopicName(descr.GetName());
        pqTabletConfig->SetDC("dc1");
        pqTabletConfig->SetTopicPath(NKikimr::JoinPath({"/Root/PQ", descr.GetName()}));
        for (auto i = 0u; i <  descr.GetPartitionPerTablet(); i++) {
            auto* part = descr.AddPartitions();
            part->SetPartitionId(i);
            part->SetTabletId(MakeTabletID(false, topicId * 100 + 1 + i));
        }
        entry.PQGroupInfo.Reset(pqInfo);

        return entry;
    }

    TSchemeCacheNavigate::TResultSet MakeResultSet(bool valid = true) {
        TSchemeCacheNavigate::TResultSet resultSet;
        if (valid) {
            resultSet.emplace_back(std::move(
                    MakeEntry(1)
            ));
            resultSet.emplace_back(std::move(
                    MakeEntry(2)
            ));
        }
        return resultSet;
    }

    void HandlesTimeout() {
        EXPECT_CALL(GetMockPQMetaCache(), HandleDescribeAllTopics(_, _)); // gets request and doesn't reply
        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);
        Runtime->EnableScheduleForActor(Actor->SelfId());

        TDispatchOptions options;
        options.FinalEvents.emplace_back([](IEventHandle& h) { return h.Type == TEvPqMetaCache::TEvDescribeAllTopicsRequest::EventType; }, 1);
        Runtime->DispatchEvents(options);

        Runtime->UpdateCurrentTime(Runtime->GetCurrentTime() + TDuration::MilliSeconds(90000 + 1));

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::ERROR, {"Marker# PQ11", "Marker# PQ16"}, MSTATUS_TIMEOUT);
    }

    void FailsOnFailedGetAllTopicsRequest() {
        GetMockPQMetaCache().SetAllTopicsAnswer(false);

        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::UNKNOWN_TOPIC, {"Marker# PQ15", "Marker# PQ17"});
    }

    void FailsOnNotOkStatusInGetNodeRequest() {
        auto entry = MakeEntry(1);
        entry.Status = TSchemeCacheNavigate::EStatus::PathErrorUnknown;

        GetMockPQMetaCache().SetAllTopicsAnswer(true, TSchemeCacheNavigate::TResultSet{entry});

        NKikimrClient::TPersQueueRequest request = MakeValidRequest(1);
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::UNKNOWN_TOPIC, "Marker# PQ150");
    }

    void FailsOnBadRootStatusInGetNodeRequest() {
        auto resultSet = MakeResultSet();
        resultSet[0].Status = ESchemeStatus::RootUnknown;

        GetMockPQMetaCache().SetAllTopicsAnswer(true, std::move(resultSet));

        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::UNKNOWN_TOPIC, {"Marker# PQ1", "Marker# PQ14"});
    }

    void FailesOnNotATopic() {
        auto resultSet = MakeResultSet();
        resultSet[1].Kind = TSchemeCacheNavigate::KindPath;
        resultSet[1].PQGroupInfo = nullptr;

        GetMockPQMetaCache().SetAllTopicsAnswer(true, std::move(resultSet));

        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::UNKNOWN_TOPIC, {"Marker# PQ95", "Marker# PQ13"});
    }

    void FailsOnNoBalancerInGetNodeRequest() {
        auto resultSet = MakeResultSet();
        SetBalancerId(resultSet, 0, Nothing());
        GetMockPQMetaCache().SetAllTopicsAnswer(true, std::move(resultSet));

        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::UNKNOWN_TOPIC, {"Marker# PQ93", "Marker# PQ193"});
    }

    void FailsOnZeroBalancerTabletIdInGetNodeRequest() {
        auto resultSet = MakeResultSet();
        SetBalancerId(resultSet, 0, 0);
        GetMockPQMetaCache().SetAllTopicsAnswer(true, std::move(resultSet));

        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::UNKNOWN_TOPIC, {"Marker# PQ94", "Marker# PQ22"});
    }

    void FailsOnBalancerDescribeResultFailureWhenTopicsAreGivenExplicitly() {
        auto resultSet = MakeResultSet();
        resultSet[1].Status = TSchemeCacheNavigate::EStatus::LookupError;
        //SetBalancerId(resultSet, 1, 0);
        GetMockPQMetaCache().SetAllTopicsAnswer(true, resultSet);

        NKikimrClient::TPersQueueRequest request = MakeValidRequest();
        RegisterActor(request);

        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::ERROR, "Marker# PQ1");

    }

    // Implementation details for test with pipe disconnection for inheritance
    enum class EDisconnectionMode {
        DisconnectionComesFirst,
        DisconnectionComesSecond,
        AnswerDoesNotArrive,
    };

    template <class TResponseEvent>
    void HandlesPipeDisconnectionImpl(EDisconnectionMode disconnectionMode, std::function<void(EDisconnectionMode disconnectionMode)> dataValidationFunction, bool requestTheWholeTopic = false) {
        GetMockPQMetaCache().SetAllTopicsAnswer(true, std::forward<TSchemeCacheNavigate::TResultSet>(MakeResultSet()));

        PrepareBalancer(topic1, MakeTabletID(false, 100), {{1, MakeTabletID(false, 101)}});
        PreparePQTablet(topic1, MakeTabletID(false, 101), {0});

        PrepareBalancer(topic2, MakeTabletID(false, 200), {{1, MakeTabletID(false, 201)}, {2, MakeTabletID(false, 202)}, {3, MakeTabletID(false, 203)}});
        PreparePQTablet(topic2, MakeTabletID(false, 201), {0});
        PreparePQTablet(topic2, MakeTabletID(false, 202), {1});
        PreparePQTablet(topic2, MakeTabletID(false, 203), {2});

        const ui64 tabletToDestroy = MakeTabletID(false, 203);

        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        RegisterActor(req);

        // Pause responses with status
        PauseInputForTestActors<TResponseEvent>();
        const size_t expectedEventsCount = requestTheWholeTopic ? 4 : 3; // When request is about the whole topic (not for particular partitions),
                                                                         // we expect one more event for partition 0 of topic2
        // Wait that pause events are sent
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition([this, expectedEventsCount](IEventHandle&){ return PausedEvents.size() == expectedEventsCount; }));
            Runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(PausedEvents.size(), expectedEventsCount);

        // Destroy one tablet and wait corresponding event from pipe
        PauseInputForTestActors(NKikimr::TEvTabletPipe::EvClientDestroyed);
        Runtime->SendToPipe(tabletToDestroy, EdgeActorId, new TEvents::TEvPoisonPill());
        {
            TDispatchOptions options;
            options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition([this, expectedEventsCount](IEventHandle&){ return PausedEvents.size() == expectedEventsCount + 1; }));
            Runtime->DispatchEvents(options);
        }
        UNIT_ASSERT_VALUES_EQUAL(PausedEvents.size(), expectedEventsCount + 1);

        // Save paused events locally
        // Later we will change order of them
        std::list<TAutoPtr<IEventHandle>> pausedEvents;
        pausedEvents.swap(PausedEvents);
        ResumeEventsForTestActors();

        TAutoPtr<IEventHandle> disconnectEvent = pausedEvents.back();
        pausedEvents.pop_back();
        TAutoPtr<IEventHandle> answerEvent;
        {
            bool foundAnswerEvent = false;
            for (auto i = pausedEvents.begin(); i != pausedEvents.end(); ++i) {
                auto& ev = *i;
                UNIT_ASSERT(ev.Get() != nullptr);
                if (GetTabletId(ev->Get<TResponseEvent>()) == tabletToDestroy) {
                    foundAnswerEvent = true;
                    answerEvent = ev;
                    i = pausedEvents.erase(i);
                }
            }
            UNIT_ASSERT(foundAnswerEvent);
        }

        switch (disconnectionMode) {
        case EDisconnectionMode::DisconnectionComesFirst:
            Runtime->Send(disconnectEvent.Release());
            Runtime->Send(answerEvent.Release());
            break;
        case EDisconnectionMode::DisconnectionComesSecond:
            Runtime->Send(answerEvent.Release());
            Runtime->Send(disconnectEvent.Release());
            break;
        case EDisconnectionMode::AnswerDoesNotArrive:
            Runtime->Send(disconnectEvent.Release());
            break;
        default:
            UNIT_FAIL("Unknown disconnection mode");
        }

        // Resend the rest paused events
        for (auto& ev : pausedEvents) {
            UNIT_ASSERT(ev.Get() != nullptr);
            Runtime->Send(ev.Release());
        }
        pausedEvents.clear();

        GrabResponseEvent();

        // Validate result
        dataValidationFunction(disconnectionMode);
    }
};

class TMessageBusServerPersQueueGetTopicMetadataMetaRequestTest: public TMessageBusServerPersQueueRequestCommonTest {
public:
    UNIT_TEST_SUITE(TMessageBusServerPersQueueGetTopicMetadataMetaRequestTest)
    COMMON_TESTS_LIST()
    UNIT_TEST(FailsOnEmptyTopicName)
    UNIT_TEST(SuccessfullyReplies)
    UNIT_TEST_SUITE_END();

    NKikimrClient::TPersQueueRequest MakeValidRequest(ui64 topicsCount = 2) override {
        NKikimrClient::TPersQueueRequest persQueueRequest;
        persQueueRequest.SetTicket("client_id@" BUILTIN_ACL_DOMAIN);

        auto& req = *persQueueRequest.MutableMetaRequest()->MutableCmdGetTopicMetadata();
        req.AddTopic(topic1);
        if (topicsCount > 1)
            req.AddTopic(topic2);
        return persQueueRequest;
    }

    void FailsOnEmptyTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        req.MutableMetaRequest()->MutableCmdGetTopicMetadata()->AddTopic("");
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "empty topic in GetTopicMetadata request");
    }

    void SuccessfullyReplies() {
        GetMockPQMetaCache().SetAllTopicsAnswer(true, MakeResultSet());
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        RegisterActor(req);

        GrabResponseEvent();
        AssertSucceededResponse();

        const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
        UNIT_ASSERT_C(resp->Record.GetMetaResponse().HasCmdGetTopicMetadataResult(), "Response: " << resp->Record);

        const auto& res = resp->Record.GetMetaResponse().GetCmdGetTopicMetadataResult();
        UNIT_ASSERT_VALUES_EQUAL_C(res.TopicInfoSize(), 2, "Response: " << resp->Record);

        {
            const auto& topic1Cfg = res.GetTopicInfo(0).GetTopic() == topic1 ? res.GetTopicInfo(0) : res.GetTopicInfo(1);
            UNIT_ASSERT_STRINGS_EQUAL_C(topic1Cfg.GetTopic(), topic1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Cfg.GetNumPartitions(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_C(topic1Cfg.HasConfig(), "Response: " << resp->Record);
            UNIT_ASSERT_C(topic1Cfg.GetConfig().HasVersion(), "Response: " << resp->Record);
        }

        {
            const auto& topic2Cfg = res.GetTopicInfo(0).GetTopic() == topic2 ? res.GetTopicInfo(0) : res.GetTopicInfo(1);
            UNIT_ASSERT_STRINGS_EQUAL_C(topic2Cfg.GetTopic(), topic2, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(topic2Cfg.GetNumPartitions(), 3, "Response: " << resp->Record);
            UNIT_ASSERT_C(topic2Cfg.HasConfig(), "Response: " << resp->Record);
            UNIT_ASSERT_C(topic2Cfg.GetConfig().HasVersion(), "Response: " << resp->Record);
        }
    }
};

void FillValidTopicRequest(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request, ui64 topicsCount = 2) {
    {
        auto& topic1Cfg = *request.Add();
        topic1Cfg.SetTopic(topic1);
    }

    if (topicsCount > 1){
        auto& topic2Cfg = *request.Add();
        topic2Cfg.SetTopic(topic2);
        topic2Cfg.AddPartition(1);
        topic2Cfg.AddPartition(2);
    }
}

void MakeEmptyTopic(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request) {
    UNIT_ASSERT_UNEQUAL(request.size(), 0); // filled in
    request.Mutable(0)->SetTopic("");
}

void MakeDuplicatedTopic(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request) {
    UNIT_ASSERT_UNEQUAL(request.size(), 0); // filled in
    request.Mutable(1)->SetTopic(topic1);
}

void MakeDuplicatedPartition(NProtoBuf::RepeatedPtrField<::NKikimrClient::TPersQueueMetaRequest::TTopicRequest>& request) {
    UNIT_ASSERT_UNEQUAL(request.size(), 0); // filled in
    request.Mutable(1)->AddPartition(2);
}

class TMessageBusServerPersQueueGetPartitionLocationsMetaRequestTest: public TMessageBusServerPersQueueRequestCommonTest {
public:
    UNIT_TEST_SUITE(TMessageBusServerPersQueueGetPartitionLocationsMetaRequestTest)
    COMMON_TESTS_LIST()
    UNIT_TEST(FailsOnEmptyTopicName)
    UNIT_TEST(FailsOnDuplicatedTopicName)
    UNIT_TEST(FailsOnDuplicatedPartition)
    UNIT_TEST(SuccessfullyPassesResponsesFromTablets)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesFirst)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesSecond)
    UNIT_TEST(HandlesPipeDisconnection_AnswerDoesNotArrive)
    UNIT_TEST_SUITE_END();

    NKikimrClient::TPersQueueRequest MakeValidRequest(ui64 topicsCount = 2) override {
        NKikimrClient::TPersQueueRequest persQueueRequest;
        persQueueRequest.SetTicket("client_id@" BUILTIN_ACL_DOMAIN);

        auto& req = *persQueueRequest.MutableMetaRequest()->MutableCmdGetPartitionLocations();
        FillValidTopicRequest(*req.MutableTopicRequest(), topicsCount);
        return persQueueRequest;
    }

    void FailsOnEmptyTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeEmptyTopic(*req.MutableMetaRequest()->MutableCmdGetPartitionLocations()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "TopicRequest must have Topic field");
    }

    void FailsOnDuplicatedTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeDuplicatedTopic(*req.MutableMetaRequest()->MutableCmdGetPartitionLocations()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "multiple TopicRequest");
    }

    void FailsOnDuplicatedPartition() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeDuplicatedPartition(*req.MutableMetaRequest()->MutableCmdGetPartitionLocations()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "multiple partition");
    }

    void SuccessfullyPassesResponsesFromTablets() {

        GetMockPQMetaCache().SetAllTopicsAnswer(true, MakeResultSet());
        PrepareBalancer(topic1, MakeTabletID(false, 100), {{1, MakeTabletID(false, 101)}});
        PreparePQTablet(topic1, MakeTabletID(false, 101), {0});

        PrepareBalancer(topic2, MakeTabletID(false, 200), {{1, MakeTabletID(false, 201)}, {2, MakeTabletID(false, 202)}, {3, MakeTabletID(false, 203)}});
        // Don't prepare partition 0 because it is not required in request
        // Don't prepare partition 1 to ensure that response is successfull despite the tablet is down
        PreparePQTablet(topic2, MakeTabletID(false, 203), {2});

        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        RegisterActor(req);
        GrabResponseEvent();
        AssertSucceededResponse();

        // Check response
        const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
        UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetPartitionLocationsResult());
        auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetPartitionLocationsResult().GetTopicResult();
        UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);

        {
            const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionLocationSize(), 1, "Response: " << resp->Record);
            const auto& partition1 = topic1Result.GetPartitionLocation(0);
            UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 0, "Response: " << resp->Record);
            UNIT_ASSERT_EQUAL_C(partition1.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(!partition1.HasErrorReason(), "Response: " << resp->Record);
            UNIT_ASSERT_C(!partition1.GetHost().empty(), "Response: " << resp->Record);
        }
        {
            const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
            UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionLocationSize(), 2, "Response: " << resp->Record);

            // Partitions (order is not specified)
            const auto& partition1 = topic2Result.GetPartitionLocation(0).GetPartition() == 1 ? topic2Result.GetPartitionLocation(0) : topic2Result.GetPartitionLocation(1);
            const auto& partition2 = topic2Result.GetPartitionLocation(0).GetPartition() == 2 ? topic2Result.GetPartitionLocation(0) : topic2Result.GetPartitionLocation(1);
            UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

            UNIT_ASSERT_UNEQUAL_C(partition1.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(partition1.HasErrorReason(), "Response: " << resp->Record);
            UNIT_ASSERT_C(partition1.GetHost().empty(), "Response: " << resp->Record); // No data

            UNIT_ASSERT_EQUAL_C(partition2.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(!partition2.HasErrorReason(), "Response: " << resp->Record);
            UNIT_ASSERT_C(!partition2.GetHost().empty(), "Response: " << resp->Record); // Data was passed
        }
    }

    void HandlesPipeDisconnection_DisconnectionComesFirst() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesFirst);
    }

    void HandlesPipeDisconnection_DisconnectionComesSecond() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesSecond);
    }

    void HandlesPipeDisconnection_AnswerDoesNotArrive() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::AnswerDoesNotArrive);
    }

    void HandlesPipeDisconnectionImpl(EDisconnectionMode disconnectionMode) {
        auto validation = [this](EDisconnectionMode disconnectionMode) {
            AssertSucceededResponse();

            // Check response
            const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
            UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetPartitionLocationsResult());
            auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetPartitionLocationsResult().GetTopicResult();
            UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);

            Cerr << "RESPONSE " << resp->Record.DebugString() << "\n";

            {
                const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
                UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionLocationSize(), 1, "Response: " << resp->Record);
                const auto& partition1 = topic1Result.GetPartitionLocation(0);
                UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 0, "Response: " << resp->Record);
                UNIT_ASSERT_EQUAL_C(partition1.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(!partition1.HasErrorReason(), "Response: " << resp->Record);
                UNIT_ASSERT_C(!partition1.GetHost().empty(), "Response: " << resp->Record);
            }
            {
                const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
                UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionLocationSize(), 2, "Response: " << resp->Record);

                // Partitions (order is not specified)
                const auto& partition1 = topic2Result.GetPartitionLocation(0).GetPartition() == 1 ? topic2Result.GetPartitionLocation(0) : topic2Result.GetPartitionLocation(1);
                const auto& partition2 = topic2Result.GetPartitionLocation(0).GetPartition() == 2 ? topic2Result.GetPartitionLocation(0) : topic2Result.GetPartitionLocation(1);
                UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
                UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

                UNIT_ASSERT_EQUAL_C(partition1.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(!partition1.HasErrorReason(), "Response: " << resp->Record);
                UNIT_ASSERT_C(!partition1.GetHost().empty(), "Response: " << resp->Record);

                if (disconnectionMode == EDisconnectionMode::DisconnectionComesSecond) {
                    UNIT_ASSERT_EQUAL_C(partition2.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                    UNIT_ASSERT_C(!partition2.HasErrorReason(), "Response: " << resp->Record);
                    UNIT_ASSERT_C(!partition2.GetHost().empty(), "Response: " << resp->Record); // Data was passed
                } else {
                    UNIT_ASSERT_EQUAL_C(partition2.GetErrorCode(), NPersQueue::NErrorCode::INITIALIZING, "Response: " << resp->Record);
                    UNIT_ASSERT_C(partition2.HasErrorReason(), "Response: " << resp->Record);
                    UNIT_ASSERT_C(partition2.GetHost().empty(), "Response: " << resp->Record); // Data was passed
                }
            }
        };
        TMessageBusServerPersQueueRequestCommonTest::HandlesPipeDisconnectionImpl<TEvTabletPipe::TEvClientConnected>(disconnectionMode, validation);
    }
};

class TMessageBusServerPersQueueGetPartitionOffsetsMetaRequestTest: public TMessageBusServerPersQueueRequestCommonTest {
public:
    UNIT_TEST_SUITE(TMessageBusServerPersQueueGetPartitionOffsetsMetaRequestTest)
    COMMON_TESTS_LIST()
    UNIT_TEST(FailsOnEmptyTopicName)
    UNIT_TEST(FailsOnDuplicatedTopicName)
    UNIT_TEST(FailsOnDuplicatedPartition)
    UNIT_TEST(SuccessfullyPassesResponsesFromTablets)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesFirst)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesSecond)
    UNIT_TEST(HandlesPipeDisconnection_AnswerDoesNotArrive)
    UNIT_TEST_SUITE_END();

    NKikimrClient::TPersQueueRequest MakeValidRequest(ui64 topicsCount = 2) override {
        NKikimrClient::TPersQueueRequest persQueueRequest;
        persQueueRequest.SetTicket("client_id@" BUILTIN_ACL_DOMAIN);

        auto& req = *persQueueRequest.MutableMetaRequest()->MutableCmdGetPartitionOffsets();
        FillValidTopicRequest(*req.MutableTopicRequest(), topicsCount);
        return persQueueRequest;
    }

    void FailsOnEmptyTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeEmptyTopic(*req.MutableMetaRequest()->MutableCmdGetPartitionOffsets()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "TopicRequest must have Topic field");
    }

    void FailsOnDuplicatedTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeDuplicatedTopic(*req.MutableMetaRequest()->MutableCmdGetPartitionOffsets()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "multiple TopicRequest");
    }

    void FailsOnDuplicatedPartition() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeDuplicatedPartition(*req.MutableMetaRequest()->MutableCmdGetPartitionOffsets()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "multiple partition");
    }

    void SuccessfullyPassesResponsesFromTablets() {
        GetMockPQMetaCache().SetAllTopicsAnswer(true, MakeResultSet());
        PrepareBalancer(topic1, MakeTabletID(false, 100), {{1, MakeTabletID(false, 101)}});
        PreparePQTablet(topic1, MakeTabletID(false, 101), {0});

        PrepareBalancer(topic2, MakeTabletID(false, 200), {{1, MakeTabletID(false, 201)}, {2, MakeTabletID(false, 202)}, {3, MakeTabletID(false, 203)}});
        // Don't prepare partition 0 because it is not required in request
        // Don't prepare partition 1 to ensure that response is successfull despite the tablet is down
        PreparePQTablet(topic2, MakeTabletID(false, 203), {2});

        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        RegisterActor(req);
        GrabResponseEvent();
        AssertSucceededResponse();

        // Check response
        const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
        UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetPartitionOffsetsResult());
        auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetPartitionOffsetsResult().GetTopicResult();
        UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);

        {
            const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionResultSize(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.GetPartitionResult(0).GetPartition(), 0, "Response: " << resp->Record);
            UNIT_ASSERT_EQUAL_C(topic1Result.GetPartitionResult(0).GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(!topic1Result.GetPartitionResult(0).HasErrorReason(), "Response: " << resp->Record);
            UNIT_ASSERT_C(topic1Result.GetPartitionResult(0).HasStartOffset(), "Response: " << resp->Record);
        }
        {
            const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
            UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionResultSize(), 2, "Response: " << resp->Record);

            // Partitions (order is not specified)
            const auto& partition1 = topic2Result.GetPartitionResult(0).GetPartition() == 1 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
            const auto& partition2 = topic2Result.GetPartitionResult(0).GetPartition() == 2 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
            UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

            UNIT_ASSERT_UNEQUAL_C(partition1.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(partition1.HasErrorReason(), "Response: " << resp->Record);
            UNIT_ASSERT_C(!partition1.HasStartOffset(), "Response: " << resp->Record); // No data

            UNIT_ASSERT_EQUAL_C(partition2.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(!partition2.HasErrorReason(), "Response: " << resp->Record);
            UNIT_ASSERT_C(partition2.HasStartOffset(), "Response: " << resp->Record); // Data was passed
        }
    }

    void HandlesPipeDisconnection_DisconnectionComesFirst() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesFirst);
    }

    void HandlesPipeDisconnection_DisconnectionComesSecond() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesSecond);
    }

    void HandlesPipeDisconnection_AnswerDoesNotArrive() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::AnswerDoesNotArrive);
    }

    void HandlesPipeDisconnectionImpl(EDisconnectionMode disconnectionMode) {
        auto validation = [this](EDisconnectionMode disconnectionMode) {
            AssertSucceededResponse();

            // Check response
            const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
            UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetPartitionOffsetsResult());
            auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetPartitionOffsetsResult().GetTopicResult();
            UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);

            Cerr << "RESPONSE " << resp->Record.DebugString() << "\n";

            {
                const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
                UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionResultSize(), 1, "Response: " << resp->Record);
                UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.GetPartitionResult(0).GetPartition(), 0, "Response: " << resp->Record);
                UNIT_ASSERT_EQUAL_C(topic1Result.GetPartitionResult(0).GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(!topic1Result.GetPartitionResult(0).HasErrorReason(), "Response: " << resp->Record);
                UNIT_ASSERT_C(topic1Result.GetPartitionResult(0).HasStartOffset(), "Response: " << resp->Record);
            }
            {
                const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
                UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionResultSize(), 2, "Response: " << resp->Record);

                // Partitions (order is not specified)
                const auto& partition1 = topic2Result.GetPartitionResult(0).GetPartition() == 1 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
                const auto& partition2 = topic2Result.GetPartitionResult(0).GetPartition() == 2 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
                UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
                UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

                UNIT_ASSERT_EQUAL_C(partition1.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(!partition1.HasErrorReason(), "Response: " << resp->Record);
                UNIT_ASSERT_C(partition1.HasStartOffset(), "Response: " << resp->Record); // Data was passed

                if (disconnectionMode == EDisconnectionMode::DisconnectionComesSecond) {
                    UNIT_ASSERT_EQUAL_C(partition2.GetErrorCode(), NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                    UNIT_ASSERT_C(!partition2.HasErrorReason(), "Response: " << resp->Record);
                    UNIT_ASSERT_C(partition2.HasStartOffset(), "Response: " << resp->Record); // Data was passed
                } else {
                    UNIT_ASSERT_EQUAL_C(partition2.GetErrorCode(), NPersQueue::NErrorCode::INITIALIZING, "Response: " << resp->Record);
                    UNIT_ASSERT_C(partition2.HasErrorReason(), "Response: " << resp->Record);
                    UNIT_ASSERT_C(!partition2.HasStartOffset(), "Response: " << resp->Record); // Data was passed
                }
            }
        };
        TMessageBusServerPersQueueRequestCommonTest::HandlesPipeDisconnectionImpl<TEvPersQueue::TEvOffsetsResponse>(disconnectionMode, validation);
    }
};

class TMessageBusServerPersQueueGetPartitionStatusMetaRequestTest: public TMessageBusServerPersQueueRequestCommonTest {
public:
    UNIT_TEST_SUITE(TMessageBusServerPersQueueGetPartitionStatusMetaRequestTest)
    COMMON_TESTS_LIST()
    UNIT_TEST(FailsOnEmptyTopicName)
    UNIT_TEST(FailsOnDuplicatedTopicName)
    UNIT_TEST(FailsOnDuplicatedPartition)
    UNIT_TEST(SuccessfullyPassesResponsesFromTablets)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesFirst)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesSecond)
    UNIT_TEST(HandlesPipeDisconnection_AnswerDoesNotArrive)
    UNIT_TEST_SUITE_END();

    NKikimrClient::TPersQueueRequest MakeValidRequest(ui64 topicsCount = 2) override {
        NKikimrClient::TPersQueueRequest persQueueRequest;
        persQueueRequest.SetTicket("client_id@" BUILTIN_ACL_DOMAIN);

        auto& req = *persQueueRequest.MutableMetaRequest()->MutableCmdGetPartitionStatus();
        FillValidTopicRequest(*req.MutableTopicRequest(), topicsCount);
        return persQueueRequest;
    }

    void FailsOnEmptyTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeEmptyTopic(*req.MutableMetaRequest()->MutableCmdGetPartitionStatus()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "TopicRequest must have Topic field");
    }

    void FailsOnDuplicatedTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeDuplicatedTopic(*req.MutableMetaRequest()->MutableCmdGetPartitionStatus()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "multiple TopicRequest");
    }

    void FailsOnDuplicatedPartition() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        MakeDuplicatedPartition(*req.MutableMetaRequest()->MutableCmdGetPartitionStatus()->MutableTopicRequest());
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "multiple partition");
    }

    void SuccessfullyPassesResponsesFromTablets() {
        GetMockPQMetaCache().SetAllTopicsAnswer(true, MakeResultSet());

        PrepareBalancer(topic1, MakeTabletID(false, 100), {{1, MakeTabletID(false, 101)}});
        PreparePQTablet(topic1, MakeTabletID(false, 101), {0});

        PrepareBalancer(topic2, MakeTabletID(false, 200), {{1, MakeTabletID(false, 201)}, {2, MakeTabletID(false, 202)}, {3, MakeTabletID(false, 203)}});
        // Don't prepare partition 0 because it is not required in request
        // Don't prepare partition 1 to ensure that response is successfull despite the tablet is down
        PreparePQTablet(topic2, MakeTabletID(false, 203), {2});

        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        RegisterActor(req);
        GrabResponseEvent();
        AssertSucceededResponse();

        // Check response
        const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
        UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetPartitionStatusResult());
        auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetPartitionStatusResult().GetTopicResult();
        UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);

        {
            const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionResultSize(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.GetPartitionResult(0).GetPartition(), 0, "Response: " << resp->Record);
        }
        {
            const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
            UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionResultSize(), 2, "Response: " << resp->Record);

            // Partitions (order is not specified)
            const auto& partition1 = topic2Result.GetPartitionResult(0).GetPartition() == 1 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
            const auto& partition2 = topic2Result.GetPartitionResult(0).GetPartition() == 2 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
            UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

            UNIT_ASSERT_EQUAL_C(partition1.GetStatus(), NKikimrPQ::TStatusResponse::STATUS_UNKNOWN, "Response: " << resp->Record);
            UNIT_ASSERT_EQUAL_C(partition2.GetStatus(), NKikimrPQ::TStatusResponse::STATUS_OK, "Response: " << resp->Record);

            UNIT_ASSERT_C(!partition1.HasLastInitDurationSeconds(), "Response: " << resp->Record); // No data
            UNIT_ASSERT_C(partition2.HasLastInitDurationSeconds(), "Response: " << resp->Record); // Data was passed
        }
    }

    void HandlesPipeDisconnection_DisconnectionComesFirst() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesFirst);
    }

    void HandlesPipeDisconnection_DisconnectionComesSecond() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesSecond);
    }

    void HandlesPipeDisconnection_AnswerDoesNotArrive() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::AnswerDoesNotArrive);
    }

    void HandlesPipeDisconnectionImpl(EDisconnectionMode disconnectionMode) {
        auto validation = [this](EDisconnectionMode disconnectionMode) {
            AssertSucceededResponse();

            // Check response
            const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
            UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetPartitionStatusResult());
            auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetPartitionStatusResult().GetTopicResult();
            UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);
            Cerr << "RESPONSE " << resp->Record.DebugString() << "\n";

            {
                const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
                UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionResultSize(), 1, "Response: " << resp->Record);
                const auto& partition = topic1Result.GetPartitionResult(0);
                UNIT_ASSERT_VALUES_EQUAL_C(partition.GetPartition(), 0, "Response: " << resp->Record);
                UNIT_ASSERT_EQUAL_C(partition.GetStatus(), NKikimrPQ::TStatusResponse::STATUS_OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(partition.HasLastInitDurationSeconds(), "Response: " << resp->Record); // Data was passed
            }
            {
                const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
                UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionResultSize(), 2, "Response: " << resp->Record);

                // Partitions (order is not specified)
                const auto& partition1 = topic2Result.GetPartitionResult(0).GetPartition() == 1 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
                const auto& partition2 = topic2Result.GetPartitionResult(0).GetPartition() == 2 ? topic2Result.GetPartitionResult(0) : topic2Result.GetPartitionResult(1);
                UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
                UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

                UNIT_ASSERT_EQUAL_C(partition1.GetStatus(), NKikimrPQ::TStatusResponse::STATUS_OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(partition1.HasLastInitDurationSeconds(), "Response: " << resp->Record); // Data was passed

                if (disconnectionMode == EDisconnectionMode::DisconnectionComesSecond) {
                    UNIT_ASSERT_EQUAL_C(partition2.GetStatus(), NKikimrPQ::TStatusResponse::STATUS_OK, "Response: " << resp->Record);
                    UNIT_ASSERT_C(partition2.HasLastInitDurationSeconds(), "Response: " << resp->Record); // Data was passed
                } else {
                    UNIT_ASSERT_EQUAL_C(partition2.GetStatus(), NKikimrPQ::TStatusResponse::STATUS_UNKNOWN, "Response: " << resp->Record);
                }
            }
        };
        TMessageBusServerPersQueueRequestCommonTest::HandlesPipeDisconnectionImpl<TEvPersQueue::TEvStatusResponse>(disconnectionMode, validation);
    }
};

class TMessageBusServerPersQueueGetReadSessionsInfoMetaRequestTest: public TMessageBusServerPersQueueRequestCommonTest {
public:
    UNIT_TEST_SUITE(TMessageBusServerPersQueueGetReadSessionsInfoMetaRequestTest)
    COMMON_TESTS_LIST()
    UNIT_TEST(FailsOnEmptyTopicName)
    UNIT_TEST(FailsOnNoClientSpecified)
    UNIT_TEST(SuccessfullyPassesResponsesFromTablets)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesFirst)
    UNIT_TEST(HandlesPipeDisconnection_DisconnectionComesSecond)
    UNIT_TEST(HandlesPipeDisconnection_AnswerDoesNotArrive)
    UNIT_TEST_SUITE_END();

    NKikimrClient::TPersQueueRequest MakeValidRequest(ui64 topicsCount = 2) override {
        NKikimrClient::TPersQueueRequest persQueueRequest;
        persQueueRequest.SetTicket("client_id@" BUILTIN_ACL_DOMAIN);

        auto& req = *persQueueRequest.MutableMetaRequest()->MutableCmdGetReadSessionsInfo();
        req.SetClientId("client_id");
        req.AddTopic(topic1);
        if (topicsCount > 1)
            req.AddTopic(topic2);
        return persQueueRequest;
    }

    void FailsOnEmptyTopicName() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        req.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->AddTopic("");
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "empty topic in GetReadSessionsInfo request");
    }

    void FailsOnNoClientSpecified() {
        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        req.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->ClearClientId();
        RegisterActor(req);
        GrabResponseEvent();
        AssertFailedResponse(NPersQueue::NErrorCode::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(GetResponse()->Record.GetErrorReason(), "No clientId specified in CmdGetReadSessionsInfo");
    }

    void SuccessfullyPassesResponsesFromTablets() {
        GetMockPQMetaCache().SetAllTopicsAnswer(true, MakeResultSet());

        PrepareBalancer(topic1, MakeTabletID(false, 100), {{1, MakeTabletID(false, 101)}});
        PreparePQTablet(topic1, MakeTabletID(false, 101), {0});

        PrepareBalancer(topic2, MakeTabletID(false, 200), {{1, MakeTabletID(false, 201)}, {2, MakeTabletID(false, 202)}, {3, MakeTabletID(false, 203)}});
        // Don't prepare partition 0 to test its failure processing
        PreparePQTablet(topic2, MakeTabletID(false, 202), {1});
        PreparePQTablet(topic2, MakeTabletID(false, 203), {2});

        NKikimrClient::TPersQueueRequest req = MakeValidRequest();
        Cerr << "REQUEST " << req.DebugString() << "\n";
        RegisterActor(req);
        GrabResponseEvent();
        AssertSucceededResponse();

        // Check response
        const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
        UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetReadSessionsInfoResult());
        auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult();
        UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);
        Cerr << "RESULT " << resp->Record.DebugString() << "\n";

        {
            const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionResultSize(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.GetPartitionResult(0).GetPartition(), 0, "Response: " << resp->Record);
        }
        {
            const auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? perTopicResults.Get(0) : perTopicResults.Get(1);
            UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
            UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionResultSize(), 3, "Response: " << resp->Record);

            // Partitions (order is not specified)
            const auto& partition1 = topic2Result.GetPartitionResult(0).GetPartition() == 1 ? topic2Result.GetPartitionResult(0)
                                                                                            : topic2Result.GetPartitionResult(1).GetPartition() == 1 ? topic2Result.GetPartitionResult(1)
                                                                                                                                                     : topic2Result.GetPartitionResult(2);
            const auto& partition2 = topic2Result.GetPartitionResult(0).GetPartition() == 2 ? topic2Result.GetPartitionResult(0)
                                                                                            : topic2Result.GetPartitionResult(1).GetPartition() == 2 ? topic2Result.GetPartitionResult(1)
                                                                                                                                                     : topic2Result.GetPartitionResult(2);
            const auto& partition3 = topic2Result.GetPartitionResult(0).GetPartition() == 0 ? topic2Result.GetPartitionResult(0)
                                                                                            : topic2Result.GetPartitionResult(1).GetPartition() == 0 ? topic2Result.GetPartitionResult(1)
                                                                                                                                                     : topic2Result.GetPartitionResult(2);

            UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
            UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

            UNIT_ASSERT_C(partition1.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(partition2.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
            UNIT_ASSERT_C(partition3.GetErrorCode() == (ui32)NPersQueue::NErrorCode::INITIALIZING, "Response: " << resp->Record);
        }
    }

    void HandlesPipeDisconnection_DisconnectionComesFirst() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesFirst);
    }

    void HandlesPipeDisconnection_DisconnectionComesSecond() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::DisconnectionComesSecond);
    }

    void HandlesPipeDisconnection_AnswerDoesNotArrive() {
        HandlesPipeDisconnectionImpl(EDisconnectionMode::AnswerDoesNotArrive);
    }

    void HandlesPipeDisconnectionImpl(EDisconnectionMode disconnectionMode) {
        auto validation = [this](EDisconnectionMode disconnectionMode) {
            AssertSucceededResponse();

            // Check response
            const TEvPersQueue::TEvResponse* resp = GetResponse(); // not nullptr is already asserted
            UNIT_ASSERT(resp->Record.GetMetaResponse().HasCmdGetReadSessionsInfoResult());
            auto perTopicResults = resp->Record.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult();
            UNIT_ASSERT_VALUES_EQUAL(perTopicResults.size(), 2);
            Cerr << "RESPONSE " << resp->Record.DebugString() << "\n"; 

            {
                const auto& topic1Result = perTopicResults.Get(0).GetTopic() == topic1 ? perTopicResults.Get(0) : perTopicResults.Get(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic1Result.GetTopic(), topic1);
                UNIT_ASSERT_VALUES_EQUAL_C(topic1Result.PartitionResultSize(), 1, "Response: " << resp->Record);
                const auto& partition = topic1Result.GetPartitionResult(0);
                UNIT_ASSERT_VALUES_EQUAL_C(partition.GetPartition(), 0, "Response: " << resp->Record);
                UNIT_ASSERT_C(partition.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK, "Response: " << resp->Record);

            }
            {
                auto& topic2Result = perTopicResults.Get(0).GetTopic() == topic2 ? *perTopicResults.Mutable(0) : *perTopicResults.Mutable(1);
                UNIT_ASSERT_STRINGS_EQUAL(topic2Result.GetTopic(), topic2);
                const size_t expectedPartitionsSize = 3;
                UNIT_ASSERT_VALUES_EQUAL_C(topic2Result.PartitionResultSize(), expectedPartitionsSize, "Response: " << resp->Record);

                // Partitions (order is not specified)
                std::sort(topic2Result.MutablePartitionResult()->begin(),
                          topic2Result.MutablePartitionResult()->end(),
                          [](const auto& p1, const auto& p2) {
                              return p1.GetPartition() < p2.GetPartition();
                          });
                const auto& partition0 = topic2Result.GetPartitionResult(0);
                const auto& partition1 = topic2Result.GetPartitionResult(1);
                const auto& partition2 = topic2Result.GetPartitionResult(2);

                UNIT_ASSERT_VALUES_EQUAL_C(partition0.GetPartition(), 0, "Response: " << resp->Record);
                UNIT_ASSERT_VALUES_EQUAL_C(partition1.GetPartition(), 1, "Response: " << resp->Record);
                UNIT_ASSERT_VALUES_EQUAL_C(partition2.GetPartition(), 2, "Response: " << resp->Record);

                UNIT_ASSERT_C(partition0.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                UNIT_ASSERT_C(partition1.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                if (disconnectionMode == EDisconnectionMode::DisconnectionComesSecond) {
                    UNIT_ASSERT_C(partition2.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK, "Response: " << resp->Record);
                } else {
                    UNIT_ASSERT_C(partition2.GetErrorCode() == (ui32)NPersQueue::NErrorCode::INITIALIZING, "Response: " << resp->Record);
                }
                Y_UNUSED(disconnectionMode);
            }
        };
        TMessageBusServerPersQueueRequestCommonTest::HandlesPipeDisconnectionImpl<TEvPersQueue::TEvOffsetsResponse>(disconnectionMode, validation, true);
    }
};

UNIT_TEST_SUITE_REGISTRATION(TMessageBusServerPersQueueGetTopicMetadataMetaRequestTest);
UNIT_TEST_SUITE_REGISTRATION(TMessageBusServerPersQueueGetPartitionLocationsMetaRequestTest);
UNIT_TEST_SUITE_REGISTRATION(TMessageBusServerPersQueueGetPartitionOffsetsMetaRequestTest);
UNIT_TEST_SUITE_REGISTRATION(TMessageBusServerPersQueueGetPartitionStatusMetaRequestTest);
UNIT_TEST_SUITE_REGISTRATION(TMessageBusServerPersQueueGetReadSessionsInfoMetaRequestTest);
} // namespace NMsgBusProxy
} // namespace NKikimr
