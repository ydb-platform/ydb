#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;

// TODO
static constexpr ui64 SS = 72057594046644480l;

NKikimrSchemeOp::TModifyScheme CreateTransaction(const TString& parentPath, ::NKikimrSchemeOp::TPersQueueGroupDescription& scheme);

TEvTx* CreateRequest(ui64 txId, NKikimrSchemeOp::TModifyScheme&& tx);

void DoRequest(TTopicSdkTestSetup& setup, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme);

void SplitPartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partition, TString boundary);

void MergePartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partitionLeft, const ui32 partitionRight);

TWriteMessage Msg(const TString& data, ui64 seqNo);

TTopicSdkTestSetup CreateSetup();

std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(TTopicClient& client, const TString& producer, std::optional<ui32> partition = std::nullopt, TString topic = TEST_TOPIC);

struct TTestReadSession {
    struct MsgInfo {
        ui64 PartitionId;
        ui64 SeqNo;
        ui64 Offset;
        TString Data;

        TReadSessionEvent::TDataReceivedEvent::TMessage Msg;
        bool Commited;
    };

    static constexpr size_t SemCount = 1;

    TTestReadSession(const TString& name, TTopicClient& client, size_t expectedMessagesCount = Max<size_t>(), bool autoCommit = true, std::set<ui32> partitions = {}, bool autoscalingSupport = true);

    void WaitAllMessages();

    void Assert(const std::set<size_t>& expected, NThreading::TFuture<std::set<size_t>> f, const TString& message);
    void WaitAndAssertPartitions(std::set<size_t> partitions, const TString& message);

    void Run();
    void Commit();

    void Close();

    std::set<size_t> GetPartitions();
    void SetOffset(ui32 partitionId, std::optional<ui64> offset);

    struct TImpl {

        TImpl(const TString& name, bool autoCommit)
            : Name(name)
            , AutoCommit(autoCommit)
            , Semaphore(name.c_str(), SemCount) {}

        TString Name;
        std::unordered_map<ui32, ui64> Offsets;

        bool AutoCommit;

        NThreading::TPromise<std::vector<MsgInfo>> DataPromise = NThreading::NewPromise<std::vector<MsgInfo>>();
        NThreading::TPromise<std::set<size_t>> PartitionsPromise = NThreading::NewPromise<std::set<size_t>>();

        std::vector<MsgInfo> ReceivedMessages;
        std::set<size_t> Partitions;
        std::optional<std::set<size_t>> ExpectedPartitions;

        std::set<size_t> EndedPartitions;
        std::vector<TReadSessionEvent::TEndPartitionSessionEvent> EndedPartitionEvents;

        TMutex Lock;
        TSemaphore Semaphore;

        std::optional<ui64> GetOffset(ui32 partitionId) const;
        void Modify(std::function<void (std::set<size_t>&)> modifier);

        void Acquire();
        void Release();

        NThreading::TFuture<std::set<size_t>> Wait(std::set<size_t> partitions, const TString& message);
    };

    std::shared_ptr<IReadSession> Session;
    std::shared_ptr<TImpl> Impl;

};

}
