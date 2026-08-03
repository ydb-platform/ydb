#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/persqueue/writer/metadata_initializers.h>
#include <ydb/core/persqueue/writer/partition_chooser_impl.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/test_server.h>

#include <ydb/core/persqueue/writer/pipe_utils.h>

using namespace NKikimr::NPQ;

static constexpr bool SMEnabled = true;
static constexpr bool SMDisabled = false;

using namespace NKikimr;
using namespace NActors;
using namespace NKikimrPQ;

void AddPartition(NKikimrSchemeOp::TPersQueueGroupDescription& conf,
                  ui32 id,
                  const std::optional<TString>&& boundaryFrom = std::nullopt,
                  const std::optional<TString>&& boundaryTo = std::nullopt,
                  std::vector<ui32> children = {}) {
    auto* p = conf.AddPartitions();
    p->SetPartitionId(id);
    p->SetTabletId(1000 + id);
    p->SetStatus(children.empty() ? NKikimrPQ::ETopicPartitionStatus::Active : NKikimrPQ::ETopicPartitionStatus::Inactive);
    if (boundaryFrom) {
        p->MutableKeyRange()->SetFromBound(boundaryFrom.value());
    }
    if (boundaryTo) {
        p->MutableKeyRange()->SetToBound(boundaryTo.value());
    }
    for(ui32 c : children) {
        p->AddChildPartitionIds(c);
    }
}

NKikimrSchemeOp::TPersQueueGroupDescription CreateConfig0(bool splitMergeEnabled) {
    NKikimrSchemeOp::TPersQueueGroupDescription result;
    NKikimrPQ::TPQTabletConfig* config =  result.MutablePQTabletConfig();

    result.SetBalancerTabletID(999);

    auto* partitionStrategy = config->MutablePartitionStrategy();
    partitionStrategy->SetMinPartitionCount(3);
    partitionStrategy->SetMaxPartitionCount(10);
    if (splitMergeEnabled) {
        partitionStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_CAN_SPLIT);
    } else {
        partitionStrategy->SetPartitionStrategyType(::NKikimrPQ::TPQTabletConfig_TPartitionStrategyType::TPQTabletConfig_TPartitionStrategyType_DISABLED);
    }


    config->SetTopicName("/Root/topic-1");
    config->SetTopicPath("/Root");

    return result;
}

NKikimrSchemeOp::TPersQueueGroupDescription CreateConfig(bool SplitMergeEnabled) {
    NKikimrSchemeOp::TPersQueueGroupDescription result = CreateConfig0(SplitMergeEnabled);

    AddPartition(result, 0, {}, "C");
    AddPartition(result, 1, "C", "F");
    AddPartition(result, 2, "F", "Z");
    AddPartition(result, 3, "Z", {}, {4});
    AddPartition(result, 4, "Z", {});

    return result;
}

Y_UNIT_TEST_SUITE(TPartitionChooserSuite) {

Y_UNIT_TEST(TBoundaryChooserTest) {
    auto config = CreateConfig(SMEnabled);

    NKikimr::NPQ::NPartitionChooser::TBoundaryChooser<NKikimr::NPQ::NPartitionChooser::TAsIsConverter> chooser(config);

    {
        auto value = chooser.GetPartition("A_SourceId");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1000);
    }

    {
        auto value = chooser.GetPartition("C_SourceId");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 1);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1001);
    }

    {
        auto value = chooser.GetPartition("D_SourceId");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 1);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1001);
    }

    {
        auto value = chooser.GetPartition("F_SourceId");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 2);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1002);
    }

    {
        auto value = chooser.GetPartition("Y_SourceId");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 2);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1002);
    }
}

Y_UNIT_TEST(TBoundaryChooser_GetTabletIdTest) {
    auto config = CreateConfig(SMEnabled);

    NKikimr::NPQ::NPartitionChooser::TBoundaryChooser chooser(config);
    UNIT_ASSERT_VALUES_EQUAL(chooser.GetPartition(0)->PartitionId, 0);
    UNIT_ASSERT_VALUES_EQUAL(chooser.GetPartition(1)->PartitionId, 1);
    UNIT_ASSERT_VALUES_EQUAL(chooser.GetPartition(2)->PartitionId, 2);

    // Not found
    UNIT_ASSERT(!chooser.GetPartition(3));
    UNIT_ASSERT(!chooser.GetPartition(666));
}

Y_UNIT_TEST(THashChooserTest) {
    auto config = CreateConfig(SMDisabled);

    NKikimr::NPQ::NPartitionChooser::THashChooser<NKikimr::NPQ::NPartitionChooser::TAsIsSharder> chooser(config);

    auto sourceId = [](size_t expectedPartition) {
        NYql::NDecimal::TUint128 m = -1;
        NYql::NDecimal::TUint128 l = m / 4;

        return AsKeyBound(l * expectedPartition + 7);
    };

    {
        auto value = chooser.GetPartition(sourceId(0));
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1000);
    }

    {
        auto value = chooser.GetPartition(sourceId(1));
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 1);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1001);
    }

    {
        auto value = chooser.GetPartition(sourceId(2));
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 2);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1002);
    }

}

Y_UNIT_TEST(THashChooser_GetTabletIdTest) {
    auto config = CreateConfig(SMDisabled);

    NKikimr::NPQ::NPartitionChooser::THashChooser chooser(config);
    UNIT_ASSERT_VALUES_EQUAL(chooser.GetPartition(0)->PartitionId, 0);
    UNIT_ASSERT_VALUES_EQUAL(chooser.GetPartition(1)->PartitionId, 1);
    UNIT_ASSERT_VALUES_EQUAL(chooser.GetPartition(2)->PartitionId, 2);

    // Not found
    UNIT_ASSERT(!chooser.GetPartition(666));
}


struct TWriteSessionMock: public NActors::TActorBootstrapped<TWriteSessionMock> {

    TEvPartitionChooser::TEvChooseResult::TPtr Result;
    TEvPartitionChooser::TEvChooseError::TPtr Error;
    NThreading::TPromise<void> Promise = NThreading::NewPromise<void>();

    void Bootstrap(const NActors::TActorContext&) {
        Become(&TThis::StateWork);
    }

    void Handle(TEvPartitionChooser::TEvChooseResult::TPtr& result) {
        Cerr << "Received TEvChooseResult: " << result->Get()->PartitionId << Endl;
        Result = result;
        Promise.SetValue();
    }

    void Handle(TEvPartitionChooser::TEvChooseError::TPtr& error) {
        Cerr << "Received TEvChooseError: " << error->Get()->ErrorMessage << Endl;
        Error = error;
        Promise.SetValue();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPartitionChooser::TEvChooseResult, Handle);
            hFunc(TEvPartitionChooser::TEvChooseError, Handle);
        }
    }
};

NPersQueue::TTopicConverterPtr CreateTopicConverter() {
    return NPersQueue::TTopicNameConverter::ForFirstClass(CreateConfig(SMDisabled).GetPQTabletConfig());
}

TWriteSessionMock* ChoosePartition(NPersQueue::TTestServer& server,
                                   const NKikimrSchemeOp::TPersQueueGroupDescription& config,
                                   const TString& sourceId,
                                   std::optional<ui32> preferedPartition = std::nullopt) {

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    TWriteSessionMock* mock = new TWriteSessionMock();

    auto chooser = NPQ::CreatePartitionChooser(config, true);
    auto graph = NPQ::MakeSharedPartitionGraph(config);

    NActors::TActorId parentId = server.GetRuntime()->Register(mock);
    server.GetRuntime()->Register(NKikimr::NPQ::CreatePartitionChooserActor<NTabletPipe::NTest::TPipeMock>(parentId,
                                                                                   config,
                                                                                   chooser,
                                                                                   graph,
                                                                                   fullConverter,
                                                                                   sourceId,
                                                                                   preferedPartition,
                                                                                   {}));

    mock->Promise.GetFuture().GetValueSync();

    return mock;

}

TWriteSessionMock* ChoosePartition(NPersQueue::TTestServer& server, bool spliMergeEnabled, const TString& sourceId, std::optional<ui32> preferedPartition = std::nullopt) {
    auto config = CreateConfig(spliMergeEnabled);
    return ChoosePartition(server, config, sourceId, preferedPartition);
}

void InitTable(NPersQueue::TTestServer& server) {
    class Initializer: public TActorBootstrapped<Initializer> {
    public:
        Initializer(NThreading::TPromise<void>&& promise)
            : Promise(std::move(promise)) {}

        void Bootstrap(const TActorContext& ctx) {
            Become(&TThis::StateWork);
            ctx.Send(
                NKikimr::NMetadata::NProvider::MakeServiceId(ctx.SelfID.NodeId()),
                new NKikimr::NMetadata::NProvider::TEvPrepareManager(NKikimr::NGRpcProxy::V1::TSrcIdMetaInitManager::GetInstant())
            );
        }

    private:
        void Handle(NMetadata::NProvider::TEvManagerPrepared::TPtr&, const TActorContext&) {
            Promise.SetValue();
            PassAway();
        }

        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NMetadata::NProvider::TEvManagerPrepared, Handle);
            }
        }

    private:
        NThreading::TPromise<void> Promise;
    };

    NThreading::TPromise<void> promise = NThreading::NewPromise<void>();
    auto future = promise.GetFuture();
    server.GetRuntime()->Register(new Initializer(std::move(promise)));
    future.GetValueSync();
}

void WriteToTable(NPersQueue::TTestServer& server, const TString& sourceId, ui32 partitionId, ui64 seqNo = 0) {
    InitTable(server);

    const auto& pqConfig = server.CleverServer->GetRuntime()->GetAppData().PQConfig;
    auto tableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                               : ESourceIdTableGeneration::SrcIdMeta2;

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    NKikimr::NPQ::NSourceIdEncoding::TEncodedSourceId encoded = NSourceIdEncoding::EncodeSrcId(
                fullConverter->GetTopicForSrcIdHash(), sourceId, tableGeneration
        );

    TString query;
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        query = TStringBuilder() << "--!syntax_v1\n"
            "UPSERT INTO `//Root/.metadata/TopicPartitionsMapping` (Hash, Topic, ProducerId, CreateTime, AccessTime, Partition, SeqNo) VALUES "
                                              "(" << encoded.KeysHash << ", \"" << fullConverter->GetClientsideName() << "\", \""
                                              << encoded.EscapedSourceId << "\", "<< TInstant::Now().MilliSeconds() << ", "
                                              << TInstant::Now().MilliSeconds() << ", " << partitionId << ", " << seqNo << ");";
    } else {
        query = TStringBuilder() << "--!syntax_v1\n"
                    "UPSERT INTO `/Root/PQ/SourceIdMeta2` (Hash, Topic, SourceId, CreateTime, AccessTime, Partition, SeqNo) VALUES ("
                  << encoded.Hash << ", \"" << fullConverter->GetClientsideName() << "\", \"" << encoded.EscapedSourceId << "\", "
                  << TInstant::Now().MilliSeconds() << ", " << TInstant::Now().MilliSeconds() << ", " << partitionId << ", " << seqNo << "); ";
    }
    Cerr << "Run query:\n" << query << Endl;
    auto scResult = server.AnnoyingClient->RunYqlDataQuery(query);
}

TMaybe<NYdb::TResultSet> SelectTable(NPersQueue::TTestServer& server, const TString& sourceId) {
    const auto& pqConfig = server.CleverServer->GetRuntime()->GetAppData().PQConfig;
    auto tableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                               : ESourceIdTableGeneration::SrcIdMeta2;

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    NKikimr::NPQ::NSourceIdEncoding::TEncodedSourceId encoded = NSourceIdEncoding::EncodeSrcId(
                fullConverter->GetTopicForSrcIdHash(), sourceId, tableGeneration
        );

    TString query;
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        query = TStringBuilder() << "--!syntax_v1\n"
            "SELECT Partition, SeqNo "
            "FROM  `//Root/.metadata/TopicPartitionsMapping` "
            "WHERE Hash = " << encoded.KeysHash <<
            "  AND Topic = \"" << fullConverter->GetClientsideName() << "\"" <<
            "  AND ProducerId = \"" << encoded.EscapedSourceId << "\"";
    } else {
        query = TStringBuilder() << "--!syntax_v1\n"
            "SELECT Partition, SeqNo "
            "FROM  `/Root/PQ/SourceIdMeta2` "
            "WHERE Hash = " << encoded.KeysHash <<
            "  AND Topic = \"" << fullConverter->GetClientsideName() << "\"" <<
            "  AND SourceId = \"" << encoded.EscapedSourceId << "\"";
    }
    Cerr << "Run query:\n" << query << Endl;
    return server.AnnoyingClient->RunYqlDataQuery(query);
}

void AssertTableEmpty(NPersQueue::TTestServer& server, const TString& sourceId) {
    auto result = SelectTable(server, sourceId);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->RowsCount(), 0, "Table must not contains SourceId='" << sourceId << "'");
}

void AssertTable(NPersQueue::TTestServer& server, const TString& sourceId, ui32 partitionId, ui64 seqNo) {
    auto result = SelectTable(server, sourceId);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->RowsCount(), 1, "Table must contains SourceId='" << sourceId << "'");

    NYdb::TResultSetParser parser(*result);
    UNIT_ASSERT(parser.TryNextRow());
    NYdb::TValueParser p(parser.GetValue(0));
    NYdb::TValueParser s(parser.GetValue(1));
    UNIT_ASSERT_VALUES_EQUAL(p.GetOptionalUint32().value(), partitionId);
    UNIT_ASSERT_VALUES_EQUAL(s.GetOptionalUint64().value(), seqNo);
}

// --- Helpers for LOGBROKER-10398 PathId migration tests ---
// These operate on the same table but let the caller specify the exact Topic column value,
// so the test can distinguish the legacy plain-name row from the PathId-encoded row.

// Writes a single row using an explicit Topic column value.
void WriteToTableWithTopic(NPersQueue::TTestServer& server, const TString& sourceId,
                           const TString& topicValue, ui32 partitionId, ui64 seqNo = 0) {
    InitTable(server);

    const auto& pqConfig = server.CleverServer->GetRuntime()->GetAppData().PQConfig;
    auto tableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                               : ESourceIdTableGeneration::SrcIdMeta2;

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    NKikimr::NPQ::NSourceIdEncoding::TEncodedSourceId encoded = NSourceIdEncoding::EncodeSrcId(
                fullConverter->GetTopicForSrcIdHash(), sourceId, tableGeneration
        );

    TString query = TStringBuilder() << "--!syntax_v1\n"
        "UPSERT INTO `//Root/.metadata/TopicPartitionsMapping` (Hash, Topic, ProducerId, CreateTime, AccessTime, Partition, SeqNo) VALUES "
                                          "(" << encoded.KeysHash << ", \"" << topicValue << "\", \""
                                          << encoded.EscapedSourceId << "\", " << TInstant::Now().MilliSeconds() << ", "
                                          << TInstant::Now().MilliSeconds() << ", " << partitionId << ", " << seqNo << ");";
    Cerr << "Run query:\n" << query << Endl;
    server.AnnoyingClient->RunYqlDataQuery(query);
}

// Selects the row for an explicit Topic column value.
TMaybe<NYdb::TResultSet> SelectTableWithTopic(NPersQueue::TTestServer& server, const TString& sourceId,
                                              const TString& topicValue) {
    const auto& pqConfig = server.CleverServer->GetRuntime()->GetAppData().PQConfig;
    auto tableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                               : ESourceIdTableGeneration::SrcIdMeta2;

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    NKikimr::NPQ::NSourceIdEncoding::TEncodedSourceId encoded = NSourceIdEncoding::EncodeSrcId(
                fullConverter->GetTopicForSrcIdHash(), sourceId, tableGeneration
        );

    TString query = TStringBuilder() << "--!syntax_v1\n"
        "SELECT Partition, SeqNo "
        "FROM  `//Root/.metadata/TopicPartitionsMapping` "
        "WHERE Hash = " << encoded.KeysHash <<
        "  AND Topic = \"" << topicValue << "\"" <<
        "  AND ProducerId = \"" << encoded.EscapedSourceId << "\"";
    Cerr << "Run query:\n" << query << Endl;
    return server.AnnoyingClient->RunYqlDataQuery(query);
}

void AssertTableTopic(NPersQueue::TTestServer& server, const TString& sourceId,
                      const TString& topicValue, ui32 partitionId, ui64 seqNo) {
    auto result = SelectTableWithTopic(server, sourceId, topicValue);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->RowsCount(), 1,
        "Table must contain row for SourceId='" << sourceId << "' Topic='" << topicValue << "'");

    NYdb::TResultSetParser parser(*result);
    UNIT_ASSERT(parser.TryNextRow());
    NYdb::TValueParser p(parser.GetValue(0));
    NYdb::TValueParser s(parser.GetValue(1));
    UNIT_ASSERT_VALUES_EQUAL(p.GetOptionalUint32().value(), partitionId);
    UNIT_ASSERT_VALUES_EQUAL(s.GetOptionalUint64().value(), seqNo);
}

void AssertTableTopicEmpty(NPersQueue::TTestServer& server, const TString& sourceId, const TString& topicValue) {
    auto result = SelectTableWithTopic(server, sourceId, topicValue);

    UNIT_ASSERT(result);
    UNIT_ASSERT_VALUES_EQUAL_C(result->RowsCount(), 0,
        "Table must not contain row for SourceId='" << sourceId << "' Topic='" << topicValue << "'");
}

// The PathId-encoded Topic value used by the chooser during/after migration.
TString PathIdTopic(ui64 pathId) {
    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    return NKikimr::NPQ::EncodeTopicWithPathId(pathId, fullConverter->GetClientsideName());
}

class TPQTabletMock: public TActor<TPQTabletMock> {
public:
    TPQTabletMock(ETopicPartitionStatus status, std::optional<ui64> seqNo)
        : TActor(&TThis::StateMockWork)
        , Status(status)
        , SeqNo(seqNo) {
    }

private:
    void Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvPersQueue::TEvResponse>();

        response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

        auto* sn = response->Record.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult()->AddSourceIdInfo();
        sn->SetSeqNo(SeqNo.value_or(0));
        sn->SetState(SeqNo ? NKikimrPQ::TMessageGroupInfo::STATE_REGISTERED : NKikimrPQ::TMessageGroupInfo::STATE_PENDING_REGISTRATION);

        ctx.Send(ev->Sender, response.Release());
    }

    void Handle(TEvPQ::TEvCheckPartitionStatusRequest::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvPQ::TEvCheckPartitionStatusResponse>();
        response->Record.SetStatus(Status);
        if (SeqNo) {
            response->Record.SetSeqNo(SeqNo.value());
        }

        ctx.Send(ev->Sender, response.Release());
    }

    STFUNC(StateMockWork) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvRequest, Handle);
            HFunc(TEvPQ::TEvCheckPartitionStatusRequest, Handle);
        }
    }

private:
    ETopicPartitionStatus Status;
    std::optional<ui64> SeqNo;
};


TPQTabletMock* CreatePQTabletMock(NPersQueue::TTestServer& server, ui32 partitionId, ETopicPartitionStatus status, std::optional<ui64> seqNo = std::nullopt) {
    TPQTabletMock* mock = new TPQTabletMock(status, seqNo);
    auto actorId = server.GetRuntime()->Register(mock);
    NKikimr::NTabletPipe::NTest::TPipeMock::Register(partitionId + 1000, actorId);
    return mock;
}

NPersQueue::TTestServer CreateServer() {
    NPersQueue::TTestServer server{};
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
    server.EnableLogs({NKikimrServices::PQ_PARTITION_CHOOSER}, NActors::NLog::PRI_TRACE);

    return server;
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_NewSourceId_Test) {
    // We check the scenario when writing is performed with a new SourceID
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "A_Source_0");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    AssertTable(server, "A_Source_0", 0, 0);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_PartitionActive_BoundaryTrue_Test) {
    // We check the partition selection scenario when we have already written with the
    // specified SourceID, the partition to which we wrote is active, and the partition
    // boundaries coincide with the distribution.
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_1", 0, 11);
    auto r = ChoosePartition(server, config, "A_Source_1");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    AssertTable(server, "A_Source_1", 0, 11);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_PartitionActive_BoundaryFalse_Test) {
    // We check the partition selection scenario when we have already written with the
    // specified SourceID, the partition to which we wrote is active, and the partition
    // boundaries is not coincide with the distribution.
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_2", 1, 13);
    auto r = ChoosePartition(server, config, "A_Source_2");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    AssertTable(server, "A_Source_2", 1, 13);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_PartitionInactive_0_Test) {
    // Boundary partition is inactive. It is configuration error - required reload of configuration.
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Inactive);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_3", 0, 13);
    auto r = ChoosePartition(server, config, "A_Source_3");

    UNIT_ASSERT(r->Error);
    AssertTable(server, "A_Source_3", 0, 13);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_PartitionInactive_1_Test) {
    // Boundary partition is inactive. It is configuration error - required reload of configuration.
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active); // Active but not written
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Inactive);

    WriteToTable(server, "A_Source_4", 1, 13);
    auto r = ChoosePartition(server, config, "A_Source_4");

    UNIT_ASSERT(r->Error);
    AssertTable(server, "A_Source_4", 1, 13);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_PartitionNotExists_Test) {
    // Old partition alredy deleted. Choose new partition by boundary and save SeqNo
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 1, {}, "F");
    AddPartition(config, 2, "F", {});
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 2, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_5", 0, 13);
    auto r = ChoosePartition(server, config, "A_Source_5");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->SeqNo, 13);
    AssertTable(server, "A_Source_5", 1, 13);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_OldPartitionExists_Test) {
    // Old partition exists. Receive SeqNo from the partition. Choose new partition by boundary and save SeqNo
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, {}, {1, 2});
    AddPartition(config, 1, {}, "F");
    AddPartition(config, 2, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Inactive, 157);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 2, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_6", 0, 13);
    auto r = ChoosePartition(server, config, "A_Source_6");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->SeqNo, 157);
    AssertTable(server, "A_Source_6", 1, 157);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_OldPartitionExists_NotWritten_Test) {
    // Old partition exists but not written. Choose new partition by boundary and save SeqNo
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, {}, {1, 2});
    AddPartition(config, 1, {}, "F");
    AddPartition(config, 2, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Inactive);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 2, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_7", 0, 13);
    auto r = ChoosePartition(server, config, "A_Source_7");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->SeqNo, 13);
    AssertTable(server, "A_Source_7", 1, 13);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_SourceId_OldPartitionExists_NotBoundary_Test) {
    // Old partition exists. Receive SeqNo from the partition. Choose new partition from children and save SeqNo
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F", { 2});
    AddPartition(config, 1, "F", {});
    AddPartition(config, 2, {}, "F");
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Inactive, 157);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 2, ETopicPartitionStatus::Active);

    WriteToTable(server, "Y_Source_7", 0, 13);
    auto r = ChoosePartition(server, config, "Y_Source_7");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 2);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->SeqNo, 157);
    AssertTable(server, "Y_Source_7", 2, 157);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PreferedPartition_Active_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "", 0);

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PreferedPartition_InactiveConfig_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, {}, {1});
    AddPartition(config, 1, {}, {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Inactive);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "", 0);

    UNIT_ASSERT(r->Error);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PreferedPartition_InactiveActor_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Inactive);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "", 0);

    UNIT_ASSERT(r->Error);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PreferedPartition_OtherPartition_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    WriteToTable(server, "A_Source_10", 0, 13);
    auto r = ChoosePartition(server, config, "A_Source_10", 1);

    UNIT_ASSERT(r->Error);
    AssertTable(server, "A_Source_10", 0, 13);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_BadSourceId_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(true);
    AddPartition(config, 0, {}, {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "base64:a***");

    UNIT_ASSERT(r->Error);
}


Y_UNIT_TEST(TPartitionChooserActor_SplitMergeDisabled_NewSourceId_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(false);
    AddPartition(config, 0);

    auto r = ChoosePartition(server, config, "A_Source");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeDisabled_RegisteredSourceId_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(false);
    AddPartition(config, 0);
    AddPartition(config, 1);

    WriteToTable(server, "A_Source", 0);
    auto r = ChoosePartition(server, config, "A_Source");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);

    WriteToTable(server, "A_Source", 1);
    r = ChoosePartition(server, config, "A_Source");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeDisabled_PreferedPartition_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(false);
    AddPartition(config, 0);
    AddPartition(config, 1);

    auto r = ChoosePartition(server, config, "A_Source", 0);

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeDisabled_BadSourceId_Test) {
    NPersQueue::TTestServer server = CreateServer();

    auto config = CreateConfig0(false);
    AddPartition(config, 0);

    auto r = ChoosePartition(server, config, "base64:a***");

    UNIT_ASSERT(r->Error);
}

// --- LOGBROKER-10398: PathId migration tests (SplitMergeEnabled) ---

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PathIdMigration_NewSourceId_DualWrites) {
    // Migration phase (flag = false): choosing a partition for a brand new SourceId must write
    // BOTH the PathId-encoded row and the legacy plain-name row so that old code can still read it.
    NPersQueue::TTestServer server = CreateServer();
    const ui64 pathId = 100500;

    auto config = CreateConfig0(true);
    config.SetPathId(pathId);
    AddPartition(config, 0, {}, {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "A_Source_PathId_New");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    // Legacy plain-name row is present for rollback safety.
    AssertTableTopic(server, "A_Source_PathId_New", fullConverter->GetClientsideName(), 0, 0);
    // PathId-encoded row is present as the primary key.
    AssertTableTopic(server, "A_Source_PathId_New", PathIdTopic(pathId), 0, 0);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PathIdMigration_LegacyRowFound_FallbackRead) {
    // Migration phase: only the legacy plain-name row exists (written by old code). The chooser must
    // find it via the fallback read and reuse the stored partition, then re-persist the PathId row.
    NPersQueue::TTestServer server = CreateServer();
    const ui64 pathId = 100501;

    auto config = CreateConfig0(true);
    config.SetPathId(pathId);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    // Simulate a mapping written by the old (pre-migration) code: legacy plain-name row only.
    WriteToTableWithTopic(server, "A_Source_PathId_Legacy", fullConverter->GetClientsideName(), 0, 42);

    auto r = ChoosePartition(server, config, "A_Source_PathId_Legacy");

    UNIT_ASSERT(r->Result);
    // Existing partition is reused via fallback read of the legacy row.
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    // After choosing, the PathId-encoded row is now populated as well (dual write).
    AssertTableTopic(server, "A_Source_PathId_Legacy", PathIdTopic(pathId), 0, 42);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PathIdMigration_RecreatedTopic_OldMappingIgnored) {
    // A topic was recreated with the same name but a different PathId. A stale row exists under the
    // OLD PathId-encoded Topic value pointing at an old partition with a non-zero SeqNo. With the new
    // PathId, the lookup must miss that stale mapping and its SeqNo must NOT leak into the new topic.
    NPersQueue::TTestServer server = CreateServer();
    const ui64 oldPathId = 111;
    const ui64 newPathId = 222;

    auto config = CreateConfig0(true);
    config.SetPathId(newPathId);
    AddPartition(config, 0, {}, "F");
    AddPartition(config, 1, "F", {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1, ETopicPartitionStatus::Active);

    // Stale row from the previous incarnation, stored under the OLD PathId-encoded Topic value.
    WriteToTableWithTopic(server, "A_Source_Recreated", PathIdTopic(oldPathId), 0, 777);

    auto r = ChoosePartition(server, config, "A_Source_Recreated");

    UNIT_ASSERT(r->Result);
    // The stale SeqNo=777 from the old PathId row must not be reused for the new topic.
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->SeqNo.value_or(0), 0);
    // The new PathId-encoded row is written with a clean SeqNo.
    AssertTableTopic(server, "A_Source_Recreated", PathIdTopic(newPathId), r->Result->Get()->PartitionId, 0);
    // The stale old-PathId row is left untouched.
    AssertTableTopic(server, "A_Source_Recreated", PathIdTopic(oldPathId), 0, 777);
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_PathIdCutover_NoLegacyWrite) {
    // Cutover phase (flag = true): only the PathId-encoded row is written; no legacy plain-name row.
    NPersQueue::TTestServer server = CreateServer();
    server.CleverServer->GetRuntime()->GetAppData().FeatureFlags
        .SetTopicPartitionMappingByPathIdMigrationComplete(true);
    const ui64 pathId = 100502;

    auto config = CreateConfig0(true);
    config.SetPathId(pathId);
    AddPartition(config, 0, {}, {});
    CreatePQTabletMock(server, 0, ETopicPartitionStatus::Active);

    auto r = ChoosePartition(server, config, "A_Source_Cutover");

    UNIT_ASSERT(r->Result);
    UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    // PathId-encoded row is written.
    AssertTableTopic(server, "A_Source_Cutover", PathIdTopic(pathId), 0, 0);
    // Legacy plain-name row is NOT written after cutover.
    AssertTableTopicEmpty(server, "A_Source_Cutover", fullConverter->GetClientsideName());
}


}
