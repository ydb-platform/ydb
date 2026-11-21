#include "dst_cdc_creator.h"
#include "dst_alterer.h"
#include "private_events.h"
#include "public_events.h"
#include "target_cdc.h"
#include "topic_mirror.h"

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/testlib/tablet_helpers/runtime.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/testlib/tx_helpers.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/persqueue/public/schema/schema.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/util/message_differencer.h>
#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/string/printf.h>
#include <util/string/hex.h>
#include <memory>

namespace NKikimr::NReplication::NController {
namespace {
using namespace NTestHelpers;
namespace TEvSchemeShard = NSchemeShard::TEvSchemeShard;

class TCdcHandoffGate: public TActorBootstrapped<TCdcHandoffGate> {
public:
    static constexpr ui64 DropBlocked = 1;
    static constexpr ui64 CompletionBlocked = 2;
    static constexpr ui64 Release = 3;

    TCdcHandoffGate(TActorId pipe, TActorId notify, bool blockDrop = true)
        : Pipe(pipe), Notify(notify), BlockDrop(blockDrop)
    {}

    void Bootstrap() { Become(&TThis::StateWork); }

private:
    void Handle(TEvPipeCache::TEvForward::TPtr& ev) {
        const auto* event = ev->Get()->Ev.Get();
        bool block = false;
        ui64 tag = DropBlocked;
        if (event->Type() == TEvSchemeShard::TEvModifySchemeTransaction::EventType) {
            const auto& record = static_cast<const TEvSchemeShard::TEvModifySchemeTransaction*>(event)->Record;
            for (const auto& tx : record.GetTransaction()) {
                block |= BlockDrop && tx.HasDropCdcStream();
                if (tx.HasAlterTable() && tx.GetAlterTable().ActivateCdcStreamsSize()) {
                    DetachTxId = record.GetTxId();
                }
            }
        } else if (event->Type() == TEvSchemeShard::TEvNotifyTxCompletion::EventType) {
            const auto& record = static_cast<const TEvSchemeShard::TEvNotifyTxCompletion*>(event)->Record;
            block = BlockCompletion && DetachTxId && record.GetTxId() == DetachTxId;
            tag = CompletionBlocked;
        }
        if (block) {
            Pending.push_back(std::move(ev));
            Send(Notify, new TEvents::TEvWakeup(tag));
        } else {
            Send(ev->Forward(Pipe));
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, Release);
        if (BlockDrop) {
            BlockDrop = false;
        } else {
            BlockCompletion = false;
        }
        for (auto& request : Pending) {
            Send(request->Forward(Pipe));
        }
        Pending.clear();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPipeCache::TEvForward, Handle);
            hFunc(TEvents::TEvWakeup, Handle);
        default:
            Send(ev->Forward(Pipe));
        }
    }

    TActorId Pipe;
    TActorId Notify;
    bool BlockDrop = true;
    bool BlockCompletion = true;
    ui64 DetachTxId = 0;
    TVector<TEvPipeCache::TEvForward::TPtr> Pending;
};

class TLiveCdcMirror {
    static constexpr const char* Consumer = "application";
    static constexpr const char* MirrorConsumer = "cdc-mirror-guid";
    static constexpr const char* SourceFeed = "/Root/source/feed";
    static constexpr const char* DestinationFeed = "/Root/replica/feed";

    static TFeatureFlags Flags() {
        TFeatureFlags flags;
        flags.FeatureFlags.SetEnableTopicReplication(true);
        flags.FeatureFlags.SetEnableTopicDiskSubDomainQuota(true);
        return flags;
    }

    template <typename TPredicate>
    static void Wait(TPredicate&& ready, TStringBuf description) {
        const auto deadline = TInstant::Now() + TDuration::Seconds(60);
        while (!ready()) {
            UNIT_ASSERT_C(TInstant::Now() < deadline, "Timed out waiting for " << description);
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    static NYdb::NTable::TSession Session(TEnv<>& env) {
        NYdb::NTable::TTableClient client(env.GetDriver());
        const auto result = client.CreateSession().GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result.GetSession();
    }

    static void Scheme(TEnv<>& env, const TString& query) {
        const auto result = Session(env).ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    static auto Execute(TEnv<>& env, const TString& query) {
        auto result = Session(env).ExecuteDataQuery(query,
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return result;
    }

    NKikimrSchemeOp::TPathDescription Describe(const TString& path) {
        auto request = MakeHolder<TEvSchemeShard::TEvDescribeScheme>(path);
        request->Record.MutableOptions()->SetShowPrivateTable(true);
        const auto response = Destination.Send<TEvSchemeShard::TEvDescribeSchemeResult>(SchemeShard, std::move(request));
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->GetRecord().GetStatus(), NKikimrScheme::StatusSuccess);
        return response->Get()->GetRecord().GetPathDescription();
    }

    static void ModifyAt(TEnv<>& env, NKikimrSchemeOp::TModifyScheme tx, NKikimrScheme::EStatus expected = NKikimrScheme::StatusAccepted) {
        const auto schemeShard = env.GetPathId("/Root").OwnerId;
        const auto allocated = env.Send<TEvTxUserProxy::TEvAllocateTxIdResult>(
            MakeTxProxyID(), new TEvTxUserProxy::TEvAllocateTxId());
        const auto txId = allocated->Get()->TxId;
        auto request = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(txId, schemeShard);
        *request->Record.AddTransaction() = std::move(tx);
        const auto result = env.Send<TEvSchemeShard::TEvModifySchemeTransactionResult>(schemeShard, std::move(request));
        UNIT_ASSERT_C(result->Get()->Record.GetStatus() == expected, result->Get()->Record.ShortDebugString());
        if (expected != NKikimrScheme::StatusAccepted) {
            return;
        }
        env.Send<TEvSchemeShard::TEvNotifyTxCompletionResult>(schemeShard,
            new TEvSchemeShard::TEvNotifyTxCompletion(txId));
    }

    void Modify(NKikimrSchemeOp::TModifyScheme tx, NKikimrScheme::EStatus expected = NKikimrScheme::StatusAccepted) {
        ModifyAt(Destination, std::move(tx), expected);
    }

    static void AddConsumer(TEnv<>& env, const TString& path, bool replication, const TString& name = Consumer) {
        NPQ::NSchema::TAlterTopicSettings settings;
        settings.Database = "/Root";
        settings.Request.set_path(path);
        auto& consumer = *settings.Request.add_add_consumers();
        consumer.set_name(replication ? MirrorConsumer : name);
        consumer.set_important(replication);
        if (replication) {
            (*settings.Request.mutable_alter_attributes())["_ydb_replication_consumer"] = MirrorConsumer;
        } else {
            consumer.set_read_speed_bytes_per_second(2_MB);
        }
        env.GetRuntime().Register(NPQ::NSchema::CreateAlterTopicActor(env.GetSender(), std::move(settings)));
        const auto result = env.GetRuntime().GrabEdgeEvent<NPQ::NSchema::TEvSchemaResponse>(env.GetSender());
        UNIT_ASSERT_C(result->Get()->Status == Ydb::StatusIds::SUCCESS, result->Get()->ErrorMessage);
    }

    void AlterMirror(TReplication::EState state) {
        NKikimrReplication::TConnectionParams params;
        params.SetEndpoint(Source->GetEndpoint());
        params.SetDatabase("/Root");
        params.MutableOAuthToken()->SetToken("root@builtin");
        Destination.GetRuntime().Register(CreateDstTopicAlterer(Destination.GetSender(), SchemeShard,
            1, 1, TopicId, state, Owner, TTopicMirrorSettings(params, SourceFeed, MirrorConsumer)));
        const auto result = Destination.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvAlterDstResult>(Destination.GetSender());
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Error);
        const auto topic = Describe(TString(DestinationFeed) + "/streamImpl");
        const auto& config = topic.GetPersQueueGroup().GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(config.GetPartitionConfig().HasMirrorFrom(), state == TReplication::EState::Ready);
    }

    void CreateCdc(TDstCdcSettings settings) {
        const bool activate = settings.SourceStream.state() == Ydb::Table::ChangefeedDescription::STATE_ENABLED;
        Destination.GetRuntime().Register(CreateDstCdcCreator(Destination.GetSender(), SchemeShard,
            "/Root", Owner, 1, 1, std::move(settings)));
        const auto result = Destination.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(Destination.GetSender());
        UNIT_ASSERT_C(result->Get()->IsSuccess(), result->Get()->Error);
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->ActivateCdcOnDetach, activate);
        UNIT_ASSERT_VALUES_UNEQUAL(result->Get()->CdcStreamPathId, result->Get()->DstPathId);
        TopicId = result->Get()->DstPathId;
        result->Get()->CdcStreamPathId.ToProto(&StreamId);
        UNIT_ASSERT(result->Get()->CdcState);
        CreatedCdc = *result->Get()->CdcState;
        UNIT_ASSERT_VALUES_UNEQUAL(CreatedCdc.GetResetCreateTxId(),
            Describe("/Root/replica/" + CreatedCdc.GetDefinition().GetStreamDescription().GetName()).GetSelf().GetCreateTxId());
    }

    struct TRecord {
        TString Data;
        ui64 Offset;
        TString Producer;
        ui64 SeqNo;
    };

    static TVector<TRecord> Read(TEnv<>& env, const TString& path, size_t count) {
        using namespace NYdb::NTopic;
        TTopicClient client(env.GetDriver());
        auto reader = client.CreateReadSession(TReadSessionSettings().WithoutConsumer()
            .AppendTopics(TTopicReadSettings(path).AppendPartitionIds(0)));
        TVector<TRecord> records;
        Wait([&] {
            auto event = reader->GetEvent();
            if (!event) {
                return false;
            }
            if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                start->Confirm(0);
            } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                for (const auto& message : data->GetMessages()) {
                    records.push_back({TString(message.GetData()), message.GetOffset(),
                        TString(message.GetProducerId()), message.GetSeqNo()});
                }
                UNIT_ASSERT_LE(records.size(), count);
            } else if (auto* closed = std::get_if<TSessionClosedEvent>(&*event)) {
                UNIT_FAIL(closed->DebugString());
            }
            return records.size() == count;
        }, path);
        UNIT_ASSERT(reader->Close(TDuration::Seconds(10)));
        return records;
    }

    NKikimrSchemeOp::TModifyScheme RemovalRequest() {
        const auto topic = Describe(TString(DestinationFeed) + "/streamImpl");
        NKikimrSchemeOp::TModifyScheme tx;
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropCdcStream);
        tx.SetInternal(true);
        Owner.ToProto(tx.MutableExpectedReplicationOwnerPathId());
        auto& drop = *tx.MutableDropCdcStream();
        drop.SetTableName("replica");
        drop.AddStreamName("feed");
        for (const auto& path : {Describe("/Root/replica"), Describe(DestinationFeed), topic}) {
            auto& condition = *tx.AddApplyIf();
            condition.SetPathId(path.GetSelf().GetPathId());
            condition.SetPathVersion(path.GetSelf().GetPathVersion());
        }
        return tx;
    }

    void Recreate(const NKikimrSchemeOp::TCreateCdcStream& manifest) {
        NKikimrSchemeOp::TModifyScheme tx;
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateCdcStream);
        tx.SetInternal(true);
        Owner.ToProto(tx.MutableExpectedReplicationOwnerPathId());
        *tx.MutableCreateCdcStream() = manifest;
        Modify(std::move(tx));
        StreamId = Describe(DestinationFeed).GetCdcStreamDescription().GetPathId();
        const auto topic = Describe(TString(DestinationFeed) + "/streamImpl");
        TopicId = TPathId(topic.GetSelf().GetSchemeshardId(), topic.GetSelf().GetPathId());
        TopicTablet = topic.GetPersQueueGroup().GetPartitions(0).GetTabletId();
    }

    TDstCdcResetSettings CaptureResetSettings() {
        return {
            .TablePathId = Destination.GetPathId("/Root/replica"),
            .StreamPathId = TPathId::FromProto(StreamId),
            .TopicPathId = TopicId,
            .CreateTxId = CreatedCdc.GetResetCreateTxId(),
            .Definition = CreatedCdc.GetDefinition(),
            .ActivateOnDetach = true,
        };
    }

    void ResetCdc(TDstCdcResetSettings settings, bool saveResult = true,
            NKikimrScheme::EStatus expected = NKikimrScheme::StatusSuccess)
    {
        Destination.GetRuntime().Register(CreateDstCdcResetter(Destination.GetSender(), SchemeShard,
            "/Root", Owner, 1, 1, std::move(settings)));
        const auto result = Destination.GetRuntime().GrabEdgeEvent<TEvPrivate::TEvCreateDstResult>(Destination.GetSender());
        UNIT_ASSERT_C(result->Get()->Status == expected, result->Get()->Error);
        if (expected != NKikimrScheme::StatusSuccess || !saveResult) {
            return;
        }
        UNIT_ASSERT(result->Get()->ActivateCdcOnDetach);
        TopicId = result->Get()->DstPathId;
        result->Get()->CdcStreamPathId.ToProto(&StreamId);
        TopicTablet = Describe(TString(DestinationFeed) + "/streamImpl").GetPersQueueGroup().GetPartitions(0).GetTabletId();
    }

    void CheckEmptyConsumers(const NKikimrPQ::TPQTabletConfig& before) {
        const auto after = Describe(TString(DestinationFeed) + "/streamImpl").GetPersQueueGroup().GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(after.ConsumersSize(), before.ConsumersSize());
        NYdb::NTopic::TTopicClient client(Destination.GetDriver());
        for (const auto& consumer : before.GetConsumers()) {
            const auto* replacement = NPQ::GetConsumer(after, consumer.GetName());
            UNIT_ASSERT(replacement);
            auto definition = *replacement;
            auto oldDefinition = consumer;
            definition.ClearVersion();
            definition.ClearGeneration();
            oldDefinition.ClearVersion();
            oldDefinition.ClearGeneration();
            UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equivalent(definition, oldDefinition));
            const auto* quota = NPQ::GetReadQuota(after, consumer.GetName());
            const auto* oldQuota = NPQ::GetReadQuota(before, consumer.GetName());
            UNIT_ASSERT(quota && oldQuota);
            UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equivalent(*quota, *oldQuota));
            Wait([&] {
                const auto result = client.DescribeConsumer(DestinationFeed, consumer.GetName(),
                    NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                const auto& partition = result.GetConsumerDescription().GetPartitions().front();
                const auto& stats = partition.GetPartitionStats();
                const auto& consumerStats = partition.GetPartitionConsumerStats();
                return stats && consumerStats && stats->GetStartOffset() == 0
                    && stats->GetEndOffset() == 0 && consumerStats->GetCommittedOffset() == 0;
            }, "empty replacement topic and fresh application offset");
        }
    }

    NKikimrReplication::TEvDescribeReplicationResult DescribeController(ui64 controllerId) {
        auto request = MakeHolder<TEvController::TEvDescribeReplication>();
        Owner.ToProto(request->Record.MutablePathId());
        Destination.GetRuntime().SendToPipe(controllerId, Destination.GetSender(), request.Release(), 0,
            NTabletPipe::TClientRetryPolicy::WithRetries());
        const auto result = Destination.GetRuntime().GrabEdgeEvent<TEvController::TEvDescribeReplicationResult>(Destination.GetSender());
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvDescribeReplicationResult::SUCCESS);
        return result->Get()->Record;
    }

    void RebootController(ui64 controllerId) {
        auto& runtime = Destination.GetRuntime();
        const auto previous = ResolveTablet(runtime, controllerId);
        Destination.SendAsync(controllerId, new TEvents::TEvPoisonPill());
        Wait([&] {
            InvalidateTabletResolverCache(runtime, controllerId);
            return ResolveTablet(runtime, controllerId) != previous;
        }, "controller restart");
        DescribeController(controllerId);
    }

    struct TCdcChild {
        NKikimrReplicationController::TCdcTarget State;
        TPathId Topic;
    };

    TCdcChild CurrentChild() {
        return {CreatedCdc, TopicId};
    }

    TCdcChild CreateDisabledSibling() {
        Scheme(*Source, "ALTER TABLE `/Root/source` ADD CHANGEFEED dormant WITH (FORMAT = 'JSON', MODE = 'UPDATES');");
        NKikimrSchemeOp::TModifyScheme disable;
        disable.SetWorkingDir("/Root");
        disable.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterCdcStream);
        disable.MutableAlterCdcStream()->SetTableName("source");
        disable.MutableAlterCdcStream()->SetStreamName("dormant");
        disable.MutableAlterCdcStream()->MutableDisable();
        ModifyAt(*Source, std::move(disable));
        AddConsumer(*Source, "/Root/source/dormant", false);
        AddConsumer(*Source, "/Root/source/dormant", true);
        NYdb::NTopic::TTopicClient client(Source->GetDriver());
        const auto topic = client.DescribeTopic("/Root/source/dormant").GetValueSync();
        UNIT_ASSERT(topic.IsSuccess());
        const auto table = Session(*Source).DescribeTable("/Root/source").GetValueSync();
        UNIT_ASSERT(table.IsSuccess());
        const auto& streams = table.GetTableDescription().GetChangefeedDescriptions();
        const auto stream = FindIf(streams, [](const auto& value) { return value.GetName() == "dormant"; });
        UNIT_ASSERT(stream != streams.end());
        TDstCdcSettings settings{
            .TablePathId = Destination.GetPathId("/Root/replica"),
            .SourceStream = {},
            .SourceTopic = topic.GetTopicDescription(),
            .Consumer = MirrorConsumer,
        };
        stream->SerializeTo(settings.SourceStream);
        const auto primaryStream = StreamId;
        const auto primaryTopic = TopicId;
        const auto primaryCdc = CreatedCdc;
        CreateCdc(settings);
        auto result = CurrentChild();
        StreamId = primaryStream;
        TopicId = primaryTopic;
        CreatedCdc = primaryCdc;
        return result;
    }

    void SeedCdcChildren(ui64 controllerId, TVector<TCdcChild> children,
            TReplication::EDstState state = TReplication::EDstState::Paused)
    {
        const auto description = DescribeController(controllerId);
        UNIT_ASSERT_VALUES_EQUAL(description.TargetsSize(), 1);
        const auto parentId = description.GetTargets(0).GetId();
        UNIT_ASSERT_VALUES_EQUAL(parentId, 1);
        TStringBuilder query;
        query << "((return (AsList ";
        ui64 childId = parentId + 1;
        for (auto& child : children) {
            child.State.SetParentTargetId(parentId);
            const auto name = child.State.GetDefinition().GetStreamDescription().GetName();
            // Seed only enrollment records: real source consumer/managed destination already
            // exist. Automatic discovery enrollment remains disabled pending DDL admission.
            query << Sprintf(R"(
                (UpdateRow 'Targets '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))) '(
                    '('Kind (Uint8 '%u)) '('SrcPath (Utf8 '"/Root/source/%s"))
                    '('DstPath (Utf8 '"/Root/replica/%s")) '('DstState (Uint8 '%u))
                    '('DstPathOwnerId (Uint64 '%lu)) '('DstPathLocalId (Uint64 '%lu))
                    '('Cdc (String 'x"%s"))))
                (UpdateRow 'SrcStreams '('('ReplicationId (Uint64 '1)) '('TargetId (Uint64 '%lu))) '(
                    '('Name (Utf8 '"%s")) '('ConsumerName (Utf8 '"%s")) '('State (Uint8 '%u))))
            )", childId, unsigned(TReplication::ETargetKind::CdcTopic), name.c_str(), name.c_str(),
                unsigned(state), child.Topic.OwnerId, child.Topic.LocalPathId,
                HexEncode(child.State.SerializeAsString()).c_str(), childId, MirrorConsumer, MirrorConsumer,
                unsigned(TReplication::EStreamState::Ready));
            ++childId;
        }
        query << Sprintf("(UpdateRow 'Replications '('('Id (Uint64 '1))) '('('NextTargetId (Uint64 '%lu))))", childId);
        query << ")))";
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(Destination.GetRuntime(), controllerId, query, result), NKikimrProto::OK);
        RebootController(controllerId);
    }

    NKikimrReplicationController::TCdcTarget ReadCdcState(ui64 controllerId, ui64 childId) {
        NKikimrMiniKQL::TResult result;
        UNIT_ASSERT_VALUES_EQUAL(LocalQuery(Destination.GetRuntime(), controllerId, Sprintf(R"((
            (let key '('('ReplicationId (Uint64 '1)) '('Id (Uint64 '%lu))))
            (return (AsList (SetResult 'Cdc (SelectRow 'Targets key '('Cdc)))))
        ))", childId), result), NKikimrProto::OK);
        NKikimrReplicationController::TCdcTarget cdc;
        UNIT_ASSERT_C(cdc.ParseFromString(result.GetValue().GetStruct(0).GetOptional().GetOptional()
            .GetStruct(0).GetOptional().GetBytes()), result.DebugString());
        return cdc;
    }


public:
    TLiveCdcMirror()
        : Source(std::make_unique<TEnv<>>(Flags()))
        , Destination(Flags())
        , SchemeShard(Destination.GetPathId("/Root").OwnerId)
    {
        auto& runtime = Destination.GetRuntime();
        runtime.RegisterService(MakeReplicationServiceId(runtime.GetNodeId(0)), runtime.Register(CreateReplicationService()));
        Scheme(*Source, "CREATE TABLE `/Root/source` (key Uint32, value Uint32, PRIMARY KEY (key));");
        Scheme(*Source, "ALTER TABLE `/Root/source` ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES');");
        AddConsumer(*Source, SourceFeed, false);
        AddConsumer(*Source, SourceFeed, true);
        Scheme(Destination, Sprintf(R"(CREATE ASYNC REPLICATION `/Root/replication` FOR `/Root/source` AS `/Root/replica`
            WITH (CONNECTION_STRING = "grpc://%s/?database=/Root", TOKEN = "root@builtin",
                CONSISTENCY_LEVEL = "ROW", INITIAL_SCAN = TRUE);)", Source->GetEndpoint().c_str()));
        Wait([&] {
            return Destination.GetDescription("/Root/replica").GetSchemeStatus() == NKikimrScheme::StatusSuccess;
        }, "replica creation");
        Owner = Destination.GetPathId("/Root/replication");

        NYdb::NTopic::TTopicClient sourceClient(Source->GetDriver());
        const auto sourceTopic = sourceClient.DescribeTopic(SourceFeed).GetValueSync();
        UNIT_ASSERT_C(sourceTopic.IsSuccess(), sourceTopic.GetIssues().ToString());
        const auto sourceTable = Session(*Source).DescribeTable("/Root/source").GetValueSync();
        UNIT_ASSERT_C(sourceTable.IsSuccess(), sourceTable.GetIssues().ToString());
        TDstCdcSettings settings{
            .TablePathId = Destination.GetPathId("/Root/replica"),
            .SourceStream = {},
            .SourceTopic = sourceTopic.GetTopicDescription(),
            .Consumer = MirrorConsumer,
        };
        const auto& streams = sourceTable.GetTableDescription().GetChangefeedDescriptions();
        const auto sourceStream = FindIf(streams, [](const auto& stream) { return stream.GetName() == "feed"; });
        UNIT_ASSERT(sourceStream != streams.end());
        sourceStream->SerializeTo(settings.SourceStream);
        CreateCdc(settings);
        settings.StreamPathId = TPathId::FromProto(StreamId);
        settings.TopicPathId = TopicId;
        CreateCdc(settings);
        const auto topic = Describe(TString(DestinationFeed) + "/streamImpl");
        UNIT_ASSERT_VALUES_EQUAL(TopicId, TPathId(topic.GetSelf().GetSchemeshardId(), topic.GetSelf().GetPathId()));
        const auto& consumers = topic.GetPersQueueGroup().GetPQTabletConfig().GetConsumers();
        UNIT_ASSERT_VALUES_EQUAL(consumers.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(consumers[0].GetName(), Consumer);
        const auto* quota = NPQ::GetReadQuota(topic.GetPersQueueGroup().GetPQTabletConfig(), Consumer);
        UNIT_ASSERT(quota);
        UNIT_ASSERT_VALUES_EQUAL(quota->GetSpeedInBytesPerSecond(), 2_MB);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetPersQueueGroup().PartitionsSize(), 1);
        TopicTablet = topic.GetPersQueueGroup().GetPartitions(0).GetTabletId();
        const auto table = Describe("/Root/replica");
        UNIT_ASSERT_VALUES_EQUAL(table.GetTable().CdcStreamsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(TPathId::FromProto(StreamId), TPathId::FromProto(table.GetTable().GetCdcStreams(0).GetPathId()));
        AlterMirror(TReplication::EState::Ready);
    }

    void ReplicateAndCommit() {
        Execute(*Source, "UPSERT INTO `/Root/source` (key, value) VALUES (1u, 10u);");
        Wait([&] {
            return Execute(Destination, "SELECT key FROM `/Root/replica` WHERE key = 1u AND value = 10u;")
                .GetResultSet(0).RowsCount() == 1;
        }, "replicated table row");
        const auto source = Read(*Source, SourceFeed, 1);
        Mirrored = Read(Destination, DestinationFeed, 1).front();
        UNIT_ASSERT_VALUES_EQUAL(Mirrored.Data, R"({"update":{"value":10},"key":[1]})");
        UNIT_ASSERT_VALUES_EQUAL(Mirrored.Offset, source.front().Offset);
        UNIT_ASSERT_VALUES_EQUAL(Mirrored.Offset, 0);
        UNIT_ASSERT_VALUES_EQUAL(Mirrored.Producer, source.front().Producer);
        UNIT_ASSERT_VALUES_EQUAL(Mirrored.SeqNo, source.front().SeqNo);

        CommitAndWait(1);
    }

    void CommitAndWait(ui64 offset) {
        NYdb::NTopic::TTopicClient sourceClient(Source->GetDriver()), destinationClient(Destination.GetDriver());
        const auto commit = sourceClient.CommitOffset(SourceFeed, 0, Consumer, offset).GetValueSync();
        UNIT_ASSERT_C(commit.IsSuccess(), commit.GetIssues().ToString());
        Wait([&] {
            const auto result = destinationClient.DescribeConsumer(DestinationFeed, Consumer,
                NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            const auto& partitions = result.GetConsumerDescription().GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);
            const auto& stats = partitions.front().GetPartitionConsumerStats();
            return stats && stats->GetCommittedOffset() == offset;
        }, "mirrored application consumer offset");
    }

    void Stop() {
        Scheme(Destination, "ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'PAUSED');");
        Wait([&] { return Destination.GetDescription("/Root/replication")
            .GetPathDescription().GetReplicationDescription().GetState().HasPaused(); }, "paused row replication");
        AlterMirror(TReplication::EState::Done);
    }

    void Promote(bool stopped = false) {
        if (!stopped) {
            Stop();
        }

        NKikimrSchemeOp::TModifyScheme tx;
        tx.SetWorkingDir("/Root");
        tx.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
        tx.SetInternal(true);
        Owner.ToProto(tx.MutableExpectedReplicationOwnerPathId());
        tx.MutableAlterTable()->SetName("replica");
        tx.MutableAlterTable()->MutableReplicationConfig()->SetMode(NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE);
        *tx.MutableAlterTable()->AddActivateCdcStreams() = StreamId;
        Modify(std::move(tx));
    }

    NKikimrSchemeOp::TModifyScheme RemoveAndRecreateStoppedStream() {
        constexpr auto rejected = NKikimrScheme::StatusPreconditionFailed;
        Modify(RemovalRequest(), rejected); // A running mirrorer cannot be removed.
        Stop();
        auto drop = RemovalRequest();
        auto invalid = drop;
        invalid.ClearApplyIf();
        Modify(invalid, rejected);
        invalid = drop;
        invalid.SetInternal(false);
        Modify(invalid, rejected);
        invalid = drop;
        invalid.MutableExpectedReplicationOwnerPathId()->SetLocalId(Owner.LocalPathId + 1);
        Modify(invalid, rejected);
        invalid = drop;
        invalid.MutableApplyIf(1)->SetPathId(0);
        Modify(invalid, rejected);
        invalid = drop;
        invalid.MutableApplyIf(2)->SetPathId(0);
        Modify(invalid, rejected);

        const auto manifest = CreatedCdc.GetDefinition();
        const auto before = Describe(TString(DestinationFeed) + "/streamImpl").GetPersQueueGroup().GetPQTabletConfig();
        const auto oldTopic = TopicId;
        const auto oldTablet = TopicTablet;
        Modify(drop);
        UNIT_ASSERT_VALUES_EQUAL(Describe("/Root/replica").GetTable().GetReplicationConfig().GetMode(),
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        const auto blockedWrite = Session(Destination).ExecuteDataQuery(
            "UPSERT INTO `/Root/replica` (key, value) VALUES (2u, 20u);",
            NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(!blockedWrite.IsSuccess(), "Native writes must remain blocked during replacement");

        // Test-owned manifest only: production must persist this before requesting removal.
        Recreate(manifest);
        UNIT_ASSERT_VALUES_UNEQUAL(TopicId, oldTopic);
        UNIT_ASSERT_VALUES_UNEQUAL(TopicTablet, oldTablet);
        Modify(drop, rejected); // A delayed request cannot remove the replacement.
        CheckEmptyConsumers(before);
        return drop;
    }

    void ResetWithoutSource(bool dropAlreadyCommitted) {
        Stop();
        const auto snapshot = CaptureResetSettings();
        const auto drop = RemovalRequest();
        const auto before = Describe(TString(DestinationFeed) + "/streamImpl").GetPersQueueGroup().GetPQTabletConfig();
        Source.reset();

        auto invalid = snapshot;
        invalid.Definition.ClearReplicationConsumers();
        ResetCdc(std::move(invalid), false, NKikimrScheme::StatusPreconditionFailed);
        UNIT_ASSERT_VALUES_EQUAL(Describe(DestinationFeed).GetSelf().GetPathId(), snapshot.StreamPathId.LocalPathId);

        if (dropAlreadyCommitted) {
            Modify(drop);
        }
        // Replay the persisted input without saving the first completion.
        ResetCdc(snapshot, false);
        const auto replacement = Describe(DestinationFeed).GetSelf().GetPathId();
        ResetCdc(snapshot);
        UNIT_ASSERT_VALUES_EQUAL(StreamId.GetLocalId(), replacement);
        UNIT_ASSERT_VALUES_UNEQUAL(TopicId, snapshot.TopicPathId);
        Modify(drop, NKikimrScheme::StatusPreconditionFailed);
        CheckEmptyConsumers(before);

        CheckNativeStart(drop);
    }

    void ControllerHandoffWithRestarts() {
        Stop();
        const auto before = Describe(TString(DestinationFeed) + "/streamImpl").GetPersQueueGroup().GetPQTabletConfig();
        const auto primary = CurrentChild();
        const auto dormant = CreateDisabledSibling();
        const auto controllerId = Destination.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        SeedCdcChildren(controllerId, {primary, dormant});
        Source.reset();

        auto& runtime = Destination.GetRuntime();
        const auto notify = runtime.AllocateEdgeActor();
        const auto pipeService = MakePipePerNodeCacheID(false);
        const auto pipe = runtime.GetLocalServiceId(pipeService);
        const auto gate = runtime.Register(new TCdcHandoffGate(pipe, notify));
        runtime.RegisterService(pipeService, gate);
        Scheme(Destination, "ALTER ASYNC REPLICATION `/Root/replication` SET (STATE = 'DONE', FAILOVER_MODE = 'FORCE');");
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(notify)->Get()->Tag, TCdcHandoffGate::DropBlocked);
        const auto snapshot = ReadCdcState(controllerId, 2);
        UNIT_ASSERT(snapshot.GetPhase() == NKikimrReplicationController::TCdcTarget::RESETTING);
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetResetCreateTxId(), primary.State.GetResetCreateTxId());
        UNIT_ASSERT_VALUES_EQUAL(snapshot.GetDefinition().SerializeAsString(), primary.State.GetDefinition().SerializeAsString());
        Wait([&] { return ReadCdcState(controllerId, 3).GetPhase()
            == NKikimrReplicationController::TCdcTarget::RESETTING; }, "serialized sibling reset");
        UNIT_ASSERT_VALUES_EQUAL(Describe("/Root/replica").GetTable().GetReplicationConfig().GetMode(),
            NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_READ_ONLY);
        RebootController(controllerId);
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(notify)->Get()->Tag, TCdcHandoffGate::DropBlocked);
        UNIT_ASSERT_VALUES_EQUAL(ReadCdcState(controllerId, 2).GetResetCreateTxId(), snapshot.GetResetCreateTxId());
        UNIT_ASSERT_VALUES_EQUAL(ReadCdcState(controllerId, 2).GetDefinition().SerializeAsString(), snapshot.GetDefinition().SerializeAsString());
        runtime.Send(new IEventHandle(gate, notify, new TEvents::TEvWakeup(TCdcHandoffGate::Release)));

        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(notify)->Get()->Tag, TCdcHandoffGate::CompletionBlocked);
        for (const auto childId : {2, 3}) {
            const auto child = ReadCdcState(controllerId, childId);
            UNIT_ASSERT(child.GetPhase() == NKikimrReplicationController::TCdcTarget::PREPARED);
            UNIT_ASSERT(!child.HasResetCreateTxId());
        }
        Wait([&] { return Describe("/Root/replica").GetTable().GetReplicationConfig().GetMode()
            == NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE; }, "atomic CDC detach");
        RebootController(controllerId);
        Wait([&] { return DescribeController(controllerId).GetState().HasDone(); }, "recovered controller DONE");
        runtime.Send(new IEventHandle(gate, notify, new TEvents::TEvWakeup(TCdcHandoffGate::Release)));
        runtime.RegisterService(pipeService, pipe);

        auto retry = MakeHolder<TEvController::TEvAlterReplication>();
        Owner.ToProto(retry->Record.MutablePathId());
        *retry->Record.MutableConfig() = Destination.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetConfig();
        retry->Record.MutableSwitchState()->MutableDone()->SetFailoverMode(
            NKikimrReplication::TReplicationState::TDone::FAILOVER_MODE_FORCE);
        runtime.SendToPipe(controllerId, Destination.GetSender(), retry.Release(), 0,
            NTabletPipe::TClientRetryPolicy::WithRetries());
        const auto result = runtime.GrabEdgeEvent<TEvController::TEvAlterReplicationResult>(Destination.GetSender());
        UNIT_ASSERT(result->Get()->Record.GetStatus() == NKikimrReplication::TEvAlterReplicationResult::SUCCESS);
        UNIT_ASSERT(DescribeController(controllerId).GetState().HasDone());

        const auto done = ReadCdcState(controllerId, 2);
        UNIT_ASSERT(done.GetPhase() == NKikimrReplicationController::TCdcTarget::FINISHED);
        StreamId = done.GetStreamPathId();
        const auto topic = Describe(TString(DestinationFeed) + "/streamImpl");
        TopicId = TPathId(topic.GetSelf().GetSchemeshardId(), topic.GetSelf().GetPathId());
        TopicTablet = topic.GetPersQueueGroup().GetPartitions(0).GetTabletId();
        UNIT_ASSERT_VALUES_UNEQUAL(TopicId, primary.Topic);
        CheckEmptyConsumers(before);
        UNIT_ASSERT_VALUES_EQUAL(Describe("/Root/replica/dormant").GetCdcStreamDescription().GetState(),
            NKikimrSchemeOp::ECdcStreamStateDisabled);
        Execute(Destination, "UPSERT INTO `/Root/replica` (key, value) VALUES (2u, 20u);");
        UNIT_ASSERT_VALUES_EQUAL(Read(Destination, DestinationFeed, 1).front().Offset, 0);
        NYdb::NTopic::TTopicClient client(Destination.GetDriver());
        const auto description = client.DescribeTopic("/Root/replica/dormant",
            NYdb::NTopic::TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT(description.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(description.GetTopicDescription().GetPartitions().front().GetPartitionStats()->GetEndOffset(), 0);
        Scheme(Destination, "DROP ASYNC REPLICATION `/Root/replication` FORCE;");
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT_VALUES_EQUAL(Describe(TString(DestinationFeed) + "/streamImpl").GetSelf().GetPathId(), TopicId.LocalPathId);
        UNIT_ASSERT_VALUES_EQUAL(Read(Destination, DestinationFeed, 1).front().Offset, 0);
    }

    void CheckSourceCleanup() {
        NYdb::NTopic::TTopicClient sourceClient(Source->GetDriver());
        for (const auto path : {SourceFeed, "/Root/source/dormant"}) {
            const auto source = sourceClient.DescribeTopic(path).GetValueSync();
            UNIT_ASSERT(source.IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(source.GetTopicDescription().GetConsumers().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(source.GetTopicDescription().GetConsumers().front().GetConsumerName(), Consumer);
        }
        const auto table = Session(*Source).DescribeTable("/Root/source").GetValueSync();
        UNIT_ASSERT(table.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(table.GetTableDescription().GetChangefeedDescriptions().size(), 2);
    }

    void DropActiveCascade() {
        const auto primary = CurrentChild();
        const auto dormant = CreateDisabledSibling();
        const auto controllerId = Destination.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        SeedCdcChildren(controllerId, {primary, dormant}, TReplication::EDstState::Ready);
        Scheme(Destination, "DROP ASYNC REPLICATION `/Root/replication` CASCADE;");
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetDescription("/Root/replica").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
        CheckSourceCleanup();
    }

    void DropActiveWithRestart(bool abandon = false) {
        const auto tableId = Destination.GetPathId("/Root/replica");
        const auto primary = CurrentChild();
        const auto dormant = CreateDisabledSibling();
        const auto controllerId = Destination.GetDescription("/Root/replication").GetPathDescription()
            .GetReplicationDescription().GetControllerId();
        SeedCdcChildren(controllerId, {primary, dormant}, TReplication::EDstState::Ready);
        if (abandon) {
            Source.reset();
        }

        auto& runtime = Destination.GetRuntime();
        const auto notify = runtime.AllocateEdgeActor();
        const auto pipeService = MakePipePerNodeCacheID(false);
        const auto pipe = runtime.GetLocalServiceId(pipeService);
        const auto gate = runtime.Register(new TCdcHandoffGate(pipe, notify, false));
        runtime.RegisterService(pipeService, gate);
        auto session = Session(Destination);
        auto dropped = session.ExecuteSchemeQuery(TStringBuilder()
            << "DROP ASYNC REPLICATION `/Root/replication`" << (abandon ? " FORCE;" : ";"));
        UNIT_ASSERT_VALUES_EQUAL(runtime.GrabEdgeEvent<TEvents::TEvWakeup>(notify)->Get()->Tag,
            TCdcHandoffGate::CompletionBlocked);
        for (const auto id : {2, 3}) {
            UNIT_ASSERT(ReadCdcState(controllerId, id).GetPhase()
                == NKikimrReplicationController::TCdcTarget::PREPARED);
        }
        Wait([&] { return Describe("/Root/replica").GetTable().GetReplicationConfig().GetMode()
            == NKikimrSchemeOp::TTableReplicationConfig::REPLICATION_MODE_NONE; }, "retained table detach");
        // DROP may delete the controller tablet as soon as recovery finishes.
        Destination.SendAsync(controllerId, new TEvents::TEvPoisonPill());
        const auto result = dropped.GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        runtime.Send(new IEventHandle(gate, notify, new TEvents::TEvWakeup(TCdcHandoffGate::Release)));
        runtime.RegisterService(pipeService, pipe);

        UNIT_ASSERT_VALUES_EQUAL(Destination.GetDescription("/Root/replication").GetSchemeStatus(),
            NKikimrScheme::StatusPathDoesNotExist);
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetPathId("/Root/replica"), tableId);
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetPathId(DestinationFeed), TPathId::FromProto(primary.State.GetStreamPathId()));
        UNIT_ASSERT_VALUES_EQUAL(Destination.GetPathId("/Root/replica/dormant"), TPathId::FromProto(dormant.State.GetStreamPathId()));
        UNIT_ASSERT_VALUES_EQUAL(Describe("/Root/replica/dormant").GetCdcStreamDescription().GetState(),
            NKikimrSchemeOp::ECdcStreamStateDisabled);
        UNIT_ASSERT(!Describe(TString(DestinationFeed) + "/streamImpl").GetPersQueueGroup()
            .GetPQTabletConfig().GetPartitionConfig().HasMirrorFrom());
        NYdb::NTopic::TTopicClient client(Destination.GetDriver());
        const auto consumer = client.DescribeConsumer(DestinationFeed, Consumer,
            NYdb::NTopic::TDescribeConsumerSettings().IncludeStats(true)).GetValueSync();
        UNIT_ASSERT(consumer.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(consumer.GetConsumerDescription().GetPartitions().front()
            .GetPartitionConsumerStats()->GetCommittedOffset(), 1);
        if (Source) {
            CheckSourceCleanup();
        }
        CheckNativeAppend();
    }

    void CheckNativeStart(NKikimrSchemeOp::TModifyScheme staleRemoval) {
        Promote(true);
        staleRemoval.ClearExpectedReplicationOwnerPathId();
        Modify(std::move(staleRemoval), NKikimrScheme::StatusPreconditionFailed);
        Execute(Destination, "UPSERT INTO `/Root/replica` (key, value) VALUES (2u, 20u);");
        const auto records = Read(Destination, DestinationFeed, 1);
        UNIT_ASSERT_VALUES_EQUAL(records.front().Data, R"({"update":{"value":20},"key":[2]})");
        UNIT_ASSERT_VALUES_EQUAL(records.front().Offset, 0);
        UNIT_ASSERT_C(records.front().Producer.StartsWith(TString(MirrorConsumer) + ":"), records.front().Producer);
    }

    void CheckNativeAppend() {
        Execute(Destination, "UPSERT INTO `/Root/replica` (key, value) VALUES (2u, 20u);");
        const auto records = Read(Destination, DestinationFeed, 2);
        UNIT_ASSERT_VALUES_EQUAL(records[0].Data, Mirrored.Data);
        UNIT_ASSERT_VALUES_EQUAL(records[0].Producer, Mirrored.Producer);
        UNIT_ASSERT_VALUES_EQUAL(records[0].SeqNo, Mirrored.SeqNo);
        UNIT_ASSERT_VALUES_EQUAL(records[1].Data, R"({"update":{"value":20},"key":[2]})");
        UNIT_ASSERT_VALUES_EQUAL(records[1].Offset, 1);
        UNIT_ASSERT_C(records[1].Producer.StartsWith(TString(MirrorConsumer) + ":"), records[1].Producer);
        UNIT_ASSERT_VALUES_UNEQUAL(records[1].Producer, Mirrored.Producer);
        UNIT_ASSERT_VALUES_EQUAL(Execute(Destination, "SELECT key FROM `/Root/replica` ORDER BY key;")
            .GetResultSet(0).RowsCount(), 2);
        const auto topic = Describe(TString(DestinationFeed) + "/streamImpl");
        UNIT_ASSERT_VALUES_EQUAL(TPathId(topic.GetSelf().GetSchemeshardId(), topic.GetSelf().GetPathId()), TopicId);
        UNIT_ASSERT_VALUES_EQUAL(topic.GetPersQueueGroup().GetPartitions(0).GetTabletId(), TopicTablet);
    }

private:
    std::unique_ptr<TEnv<>> Source;
    TEnv<> Destination;
    const ui64 SchemeShard;
    TPathId Owner;
    TPathId TopicId;
    ui64 TopicTablet = 0;
    NKikimrProto::TPathID StreamId;
    TRecord Mirrored;
    NKikimrReplicationController::TCdcTarget CreatedCdc;
};

Y_UNIT_TEST_SUITE(CdcLiveMirroring) {
    Y_UNIT_TEST(GuardedRemovalAndFreshManagedCdc) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        const auto staleRemoval = test.RemoveAndRecreateStoppedStream();
        test.CheckNativeStart(staleRemoval);
    }

    Y_UNIT_TEST(ResetReplaysWithoutSource) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        test.ResetWithoutSource(false);
    }

    Y_UNIT_TEST(ResetRecoversCommittedDropWithoutSource) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        test.ResetWithoutSource(true);
    }

    Y_UNIT_TEST(ControllerResetAndDetachRecoverOffline) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        test.ControllerHandoffWithRestarts();
    }

    Y_UNIT_TEST(DirectDropPreservesHistoryAndOffsets) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        test.DropActiveWithRestart();
    }

    Y_UNIT_TEST(DirectDropAbandonsUnavailableSource) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        test.DropActiveWithRestart(true);
    }

    Y_UNIT_TEST(CascadeDropCleansManagedSources) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        test.DropActiveCascade();
    }

    Y_UNIT_TEST(NetworkMirrorThenNativeAtomicPromotion) {
        TLiveCdcMirror test;
        test.ReplicateAndCommit();
        // CDC child lifecycle is explicitly orchestrated through production APIs;
        // the controller does not yet discover or promote these children itself.
        test.Promote();
        test.CheckNativeAppend();
    }
}
}
}
