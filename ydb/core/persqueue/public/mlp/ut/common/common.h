#pragma once

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/threading/future/async.h>

#include <util/thread/pool.h>

#include <atomic>
#include <optional>
#include <unordered_set>

namespace NKikimr::NPQ::NMLP {

using namespace NPersQueue;

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup();

// Local setup for pipe tests: UseRealThreads=false, FullInit via WaitFuture (no SDK fixture changes).
struct TMlpPipeSetup {
    std::unique_ptr<TTestServer> Server;

    NActors::TTestActorRuntime& GetRuntime() {
        return *Server->CleverServer->GetRuntime();
    }

    TDriverConfig MakeDriverConfig() const {
        TDriverConfig config;
        config.SetEndpoint(Server->Endpoint);
        config.SetDatabase("/Root");
        config.SetAuthToken("root@builtin");
        config.SetLog(std::make_unique<TStreamLogBackend>(&Cerr));
        return config;
    }
};

std::shared_ptr<TMlpPipeSetup> CreatePipeSetup();

// Shared pool for RunWithDispatch (non-template so one instance for all call sites).
IThreadPool& GetMlpPipeDispatchPool();

// Runs SDK/gRPC work off the test thread; dispatches simulated runtime until done.
template <typename TFunc>
auto RunWithDispatch(NActors::TTestActorRuntime& runtime, TFunc&& func) {
    auto future = NThreading::Async(std::forward<TFunc>(func), GetMlpPipeDispatchPool());
    return static_cast<NKikimr::TTestActorRuntime&>(runtime).WaitFuture(std::move(future));
}

TStatus CreatePipeTopic(std::shared_ptr<TMlpPipeSetup>& setup, const TString& topicName,
    const TString& consumerName, size_t partitionCount = 1);

// Write a single message via MLP writer under UseRealThreads=false.
void WriteViaMlp(std::shared_ptr<TMlpPipeSetup>& setup, const TString& topic, const TString& body);

ui64 GetTabletId(std::shared_ptr<TMlpPipeSetup>& setup, const TString& database, const TString& topic,
    ui32 partitionId = 0);

void ExecuteDDL(TTopicSdkTestSetup& setup, const TString& query);
TStatus CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName,
    NYdb::NTopic::TCreateTopicSettings& settings);
TStatus CreateTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName,
    const TString& consumerName, size_t partitionCount = 1, bool keepMessagesOrder = false,
    bool autopartitioning = false);
TStatus AlterTopic(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& topicName,
    NYdb::NTopic::TAlterTopicSettings& settings);
TActorId CreateReaderActor(NActors::TTestActorRuntime& runtime, TReaderSettings&& settings);
TActorId CreateWriterActor(NActors::TTestActorRuntime& runtime, TWriterSettings&& settings);
TActorId CreateCommitterActor(NActors::TTestActorRuntime& runtime, TCommitterSettings&& settings);
TActorId CreateUnlockerActor(NActors::TTestActorRuntime& runtime, TUnlockerSettings&& settings);
TActorId CreateMessageDeadlineChangerActor(NActors::TTestActorRuntime& runtime, TMessageDeadlineChangerSettings&& settings);
TActorId CreatePurgerActor(NActors::TTestActorRuntime& runtime, TPurgerSettings&& settings);
TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime, TDescribeSettings&& settings);
TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime, const TString& databasePath, const TString& topicPath);
THolder<TEvPQ::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime);
// Grab edge event and fail the test on timeout (never returns null).
THolder<TEvReadResponse> GetReadResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvWriteResponse> GetWriteResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvChangeResponse> GetChangeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvPurgeResponse> GetPurgeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvDescribeResponse> GetDescribeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<NDescriber::TEvDescribeTopicsResponse> GetDescriberResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));

void AssertReadError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5));
void AssertPurgeError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5));
void AssertPurgeOK(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));

void WriteMany(std::shared_ptr<TTopicSdkTestSetup> setup, const std::string& topic, ui32 partitionId, size_t messageSize, size_t messageCount);

// The function writes `messageCount` messages. For each message, it assigns one of the `groupCount` groups in round-robin order
void WriteManyGroups(const std::shared_ptr<TTopicSdkTestSetup>& setup, const std::string& topic, size_t messageSize, size_t messageCount, size_t groupCount);

ui64 GetTabletId(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic, ui32 partitionId = 0);
ui64 GetPQRBTabletId(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic);

THolder<NKikimr::TEvPQ::TEvGetMLPConsumerStateResponse> GetConsumerState(std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& database, const TString& topic, const TString& consumer, ui32 partitionId = 0);

void ReloadPQTablet(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic, ui32 partitionId = 0);
void ReloadPQRBTablet(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic);

void ModifyTopicAcl(TTopicSdkTestSetup& setup, const TString& topicName, const NACLib::TDiffACL& acl);

// Drops TEvPipeCache::TEvForward wrapping selected inner event types and injects
// TEvDeliveryProblem to the forward sender. Requires UseRealThreads=false.
// If tabletId is set, only forwards to that tablet are broken.
class TPipeBreakGuard {
public:
    TPipeBreakGuard(
        NActors::TTestActorRuntime& runtime,
        std::unordered_set<ui32> innerEventTypes,
        size_t maxBreaks = Max<size_t>(),
        std::optional<ui64> tabletId = std::nullopt);

    size_t BrokenCount() const;

private:
    std::shared_ptr<std::atomic<size_t>> Broken_;
    NActors::TTestActorRuntime::TEventObserverHolder Observer_;
};

} // namespace NKikimr::NPQ::NMLP
