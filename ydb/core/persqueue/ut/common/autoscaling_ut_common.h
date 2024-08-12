#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

static inline IOutputStream& operator<<(IOutputStream& o, const std::set<size_t>& t) {
    o << "[" << JoinRange(", ", t.begin(), t.end()) << "]";

    return o;
}

static inline IOutputStream& operator<<(IOutputStream& o, const std::unordered_map<TString, std::set<size_t>>& t) {
    o << "{";
    for (auto& [k, v] : t) {
        o << "{" << k <<" : " << v << "}, ";
    }
    o << "}";

    return o;
}

namespace NKikimr::NPQ::NTest {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;

// TODO
static constexpr ui64 SS = 72057594046644480l;

NKikimrSchemeOp::TModifyScheme CreateTransaction(const TString& parentPath, ::NKikimrSchemeOp::TPersQueueGroupDescription& scheme);

TEvTx* CreateRequest(ui64 txId, NKikimrSchemeOp::TModifyScheme&& tx);

void DoRequest(TTopicSdkTestSetup& setup, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme);
void DoRequest(NActors::TTestActorRuntime& runtime, ui64& txId, NKikimrSchemeOp::TPersQueueGroupDescription& scheme);

void SplitPartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partition, TString boundary);
void SplitPartition(NActors::TTestActorRuntime& runtime, ui64& txId, const ui32 partition, TString boundary);

void MergePartition(TTopicSdkTestSetup& setup, ui64& txId, const ui32 partitionLeft, const ui32 partitionRight);

TWriteMessage Msg(const TString& data, ui64 seqNo);

TTopicSdkTestSetup CreateSetup();

std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(TTopicClient& client, const TString& producer, std::optional<ui32> partition = std::nullopt, TString topic = TEST_TOPIC, bool useCodec = true);

enum class SdkVersion {
    Topic,
    PQv1
};

struct IMessage {
    virtual void Commit() = 0;
    virtual ~IMessage() = default;
};

struct MsgInfo {
    ui64 PartitionId;
    ui64 SeqNo;
    ui64 Offset;
    TString Data;

    std::shared_ptr<IMessage> Msg;
    bool Commited;
/*
    MsgInfo(ui64 partitionId, ui64 seqNo, ui64 offset, const TString& data, std::shared_ptr<IMessage>& msg, bool commited)
        : PartitionId(partitionId)
        , SeqNo(seqNo)
        , Offset(offset)
        , Data(data)
        , Msg(msg)
        , Commited(commited) {
    }*/
};

struct EvEndMsg {
    std::vector<ui32> AdjacentPartitionIds;
    std::vector<ui32> ChildPartitionIds;
};

struct TestReadSessionSettings {
    TString Name;
    TTopicSdkTestSetup& Setup;
    SdkVersion Sdk;

    size_t ExpectedMessagesCount = Max<size_t>();
    bool AutoCommit = true;
    std::set<ui32> Partitions = {};
    bool AutoPartitioningSupport = true;
    std::vector<TString> Topics = {TEST_TOPIC};
};

struct ITestReadSession {
    virtual void WaitAllMessages() = 0;

    virtual void Assert(const std::set<size_t>& expected, NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> f, const TString& message) = 0;
    virtual void Assert(const std::unordered_map<TString, std::set<size_t>>& expected, NThreading::TFuture<std::unordered_map<TString, std::set<size_t>>> f, const TString& message) = 0;
    virtual void WaitAndAssertPartitions(std::set<size_t> partitions, const TString& message) = 0;
    virtual void WaitAndAssert(std::unordered_map<TString, std::set<size_t>> partitions, const TString& message) = 0;

    virtual void Run() = 0;
    virtual void Commit() = 0;
    virtual void SetAutoCommit(bool value) = 0;

    virtual void Close() = 0;

    virtual std::set<size_t> GetPartitions() = 0;
    virtual std::unordered_map<TString, std::set<size_t>> GetPartitionsA() = 0;
    virtual std::vector<MsgInfo> GetReceivedMessages() = 0;
    virtual std::vector<EvEndMsg> GetEndedPartitionEvents() = 0;

    virtual void SetOffset(ui32 partitionId, std::optional<ui64> offset) = 0;
    virtual ~ITestReadSession() = default;
};

std::shared_ptr<ITestReadSession> CreateTestReadSession(TestReadSessionSettings settings);

}
