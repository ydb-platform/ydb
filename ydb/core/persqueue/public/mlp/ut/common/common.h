#pragma once

#include <ydb/core/persqueue/public/describer/describer.h>
#include <ydb/core/persqueue/public/mlp/mlp.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/tenant_runtime.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

namespace NKikimr::NPQ::NMLP {

using namespace NPersQueue;

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;

std::shared_ptr<TTopicSdkTestSetup> CreateSetup();

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
TActorId CreateDescriberActor(NActors::TTestActorRuntime& runtime, const TString& databasePath, const TString& topicPath);
THolder<TEvPQ::TEvMLPReadResponse> WaitResult(NActors::TTestActorRuntime& runtime);
THolder<TEvReadResponse> GetReadResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvWriteResponse> GetWriteResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvChangeResponse> GetChangeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<TEvPurgeResponse> GetPurgeResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));
THolder<NDescriber::TEvDescribeTopicsResponse> GetDescriberResponse(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));

void AssertReadError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5));
void AssertPurgeError(NActors::TTestActorRuntime& runtime, Ydb::StatusIds::StatusCode errorCode, const TString& message, TDuration timeout = TDuration::Seconds(5));
void AssertPurgeOK(NActors::TTestActorRuntime& runtime, TDuration timeout = TDuration::Seconds(5));

void WriteMany(std::shared_ptr<TTopicSdkTestSetup> setup, const std::string& topic, ui32 partitionId, size_t messageSize, size_t messageCount);

ui64 GetTabletId(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic, ui32 partitionId = 0);
ui64 GetPQRBTabletId(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic);

THolder<NKikimr::TEvPQ::TEvGetMLPConsumerStateResponse> GetConsumerState(std::shared_ptr<TTopicSdkTestSetup>& setup,
    const TString& database, const TString& topic, const TString& consumer, ui32 partitionId = 0);

void ReloadPQTablet(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic, ui32 partitionId = 0);
void ReloadPQRBTablet(std::shared_ptr<TTopicSdkTestSetup>& setup, const TString& database, const TString& topic);

}
