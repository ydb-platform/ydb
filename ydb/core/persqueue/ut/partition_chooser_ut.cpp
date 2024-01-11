#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/persqueue/writer/metadata_initializers.h>
#include <ydb/core/persqueue/writer/partition_chooser_impl.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>

#include <ydb/core/persqueue/writer/pipe_utils.h>

using namespace NKikimr::NPQ;

static constexpr bool SMEnabled = true;
static constexpr bool SMDisabled = false;

NKikimrSchemeOp::TPersQueueGroupDescription CreateConfig(bool SplitMergeEnabled) {
    Cerr << ">>>>> SplitMergeEnabled=" << SplitMergeEnabled << Endl;
    NKikimrSchemeOp::TPersQueueGroupDescription result;
    NKikimrPQ::TPQTabletConfig* config =  result.MutablePQTabletConfig();

    result.SetBalancerTabletID(999);

    auto* partitionStrategy = config->MutablePartitionStrategy();
    partitionStrategy->SetMinPartitionCount(3);
    partitionStrategy->SetMaxPartitionCount(SplitMergeEnabled ? 10 : 3);

    config->SetTopicName("/Root/topic-1");
    config->SetTopicPath("/Root");

    {
        auto* p = result.AddPartitions();
        p->SetPartitionId(0);
        p->SetTabletId(1000);
        p->MutableKeyRange()->SetToBound("C");
    }

    {
        auto* p = result.MutablePQTabletConfig()->AddAllPartitions();
        p->SetPartitionId(0);
        p->SetTabletId(1000);
        p->MutableKeyRange()->SetToBound("C");
    }

    {
        auto* p = result.AddPartitions();
        p->SetPartitionId(1);
        p->SetTabletId(1001);
        p->MutableKeyRange()->SetFromBound("C");
        p->MutableKeyRange()->SetToBound("F");
    }

    {
        auto* p = result.MutablePQTabletConfig()->AddAllPartitions();
        p->SetPartitionId(1);
        p->SetTabletId(1001);
        p->MutableKeyRange()->SetFromBound("C");
        p->MutableKeyRange()->SetToBound("F");
    }

    {
        auto* p = result.AddPartitions();
        p->SetPartitionId(2);
        p->SetTabletId(1002);
        p->MutableKeyRange()->SetFromBound("F");
    }

    {
        auto* p = result.MutablePQTabletConfig()->AddAllPartitions();
        p->SetPartitionId(2);
        p->SetTabletId(1002);
        p->MutableKeyRange()->SetFromBound("F");
    }

    {
        auto* p = result.AddPartitions();
        p->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Inactive);
        p->SetPartitionId(3);
        p->SetTabletId(1003);
        p->MutableKeyRange()->SetFromBound("D");
        p->AddChildPartitionIds(4);
    }

    {
        auto* p = result.MutablePQTabletConfig()->AddAllPartitions();
        p->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Inactive);
        p->SetPartitionId(3);
        p->SetTabletId(1003);
        p->MutableKeyRange()->SetFromBound("D");
        p->AddChildPartitionIds(4);
    }

    {
        auto* p = result.AddPartitions();
        p->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Active);
        p->SetPartitionId(4);
        p->SetTabletId(1004);
        p->MutableKeyRange()->SetFromBound("D");
    }

    {
        auto* p = result.MutablePQTabletConfig()->AddAllPartitions();
        p->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Active);
        p->SetPartitionId(4);
        p->SetTabletId(1004);
        p->MutableKeyRange()->SetFromBound("D");
    }

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

    {
        auto value = chooser.GetPartition("A");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 0);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1000);
    }

    {
        auto value = chooser.GetPartition("B");
        UNIT_ASSERT_VALUES_EQUAL(value->PartitionId, 1);
        UNIT_ASSERT_VALUES_EQUAL(value->TabletId, 1001);
    }

    {
        auto value = chooser.GetPartition("C");
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
    UNIT_ASSERT(!chooser.GetPartition(3));
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

TWriteSessionMock* ChoosePartition(NPersQueue::TTestServer& server, bool spliMergeEnabled, const TString& sourceId, std::optional<ui32> preferedPartition = std::nullopt) {
    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();

    TWriteSessionMock* mock = new TWriteSessionMock();

    NActors::TActorId parentId = server.GetRuntime()->Register(mock);
    server.GetRuntime()->Register(NKikimr::NPQ::CreatePartitionChooserActorM(parentId, 
                                                                                   CreateConfig(spliMergeEnabled),
                                                                                   fullConverter,
                                                                                   sourceId,
                                                                                   preferedPartition,
                                                                                   true));

    mock->Promise.GetFuture().GetValueSync();

    return mock;
}

void WriteToTable(NPersQueue::TTestServer& server, const TString& sourceId, ui32 partitionId) {
    const auto& pqConfig = server.CleverServer->GetRuntime()->GetAppData().PQConfig;
    auto tableGeneration = pqConfig.GetTopicsAreFirstClassCitizen() ? ESourceIdTableGeneration::PartitionMapping
                                                               : ESourceIdTableGeneration::SrcIdMeta2;

    Cerr << ">>>>> pqConfig.GetTopicsAreFirstClassCitizen()=" << pqConfig.GetTopicsAreFirstClassCitizen() << Endl;

    NPersQueue::TTopicConverterPtr fullConverter = CreateTopicConverter();
    NKikimr::NPQ::NSourceIdEncoding::TEncodedSourceId encoded = NSourceIdEncoding::EncodeSrcId(
                fullConverter->GetTopicForSrcIdHash(), sourceId, tableGeneration
        );

    TString query;
    if (pqConfig.GetTopicsAreFirstClassCitizen()) {
        query = TStringBuilder() << "--!syntax_v1\n"
            "UPSERT INTO `//Root/.metadata/TopicPartitionsMapping` (Hash, Topic, ProducerId, CreateTime, AccessTime, Partition) VALUES "
                                              "(" << encoded.KeysHash << ", \"" << fullConverter->GetClientsideName() << "\", \"" 
                                              << encoded.EscapedSourceId << "\", "<< TInstant::Now().MilliSeconds() << ", "
                                              << TInstant::Now().MilliSeconds() << ", " << partitionId << ");";
    } else {
        query = TStringBuilder() << "--!syntax_v1\n"
                    "UPSERT INTO `/Root/PQ/SourceIdMeta2` (Hash, Topic, SourceId, CreateTime, AccessTime, Partition) VALUES ("
                  << encoded.Hash << ", \"" << fullConverter->GetClientsideName() << "\", \"" << encoded.EscapedSourceId << "\", "
                  << TInstant::Now().MilliSeconds() << ", " << TInstant::Now().MilliSeconds() << ", " << partitionId << "); ";
    }
    Cerr << "Run query:\n" << query << Endl;
    auto scResult = server.AnnoyingClient->RunYqlDataQuery(query);
}

using namespace NKikimr;
using namespace NActors;
using namespace NKikimrPQ;

class TPQTabletMock: public TActor<TPQTabletMock> {
public:
    TPQTabletMock(ETopicPartitionStatus status)
        : TActor(&TThis::StateMockWork)
        , Status(status) {
    }

private:
    void Handle(TEvPersQueue::TEvRequest::TPtr& ev, const TActorContext& ctx) {
        auto response = MakeHolder<TEvPersQueue::TEvResponse>();

        response->Record.SetStatus(NMsgBusProxy::MSTATUS_OK);
        response->Record.SetErrorCode(NPersQueue::NErrorCode::OK);

        auto* cmd = response->Record.MutablePartitionResponse()->MutableCmdGetOwnershipResult();
        cmd->SetOwnerCookie("ower_cookie");
        cmd->SetStatus(Status);
        cmd->SetRegistered(!!SeqNo);

        auto* sn = response->Record.MutablePartitionResponse()->MutableCmdGetMaxSeqNoResult()->AddSourceIdInfo();
        sn->SetSeqNo(SeqNo.value_or(0));

        ctx.Send(ev->Sender, response.Release());
    }

    STFUNC(StateMockWork) {
        TRACE_EVENT(NKikimrServices::PQ_PARTITION_CHOOSER);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvRequest, Handle);
        }
    }

private:
    ETopicPartitionStatus Status;
    std::optional<ui64> SeqNo;
};


TPQTabletMock* CreatePQTabletMock(NPersQueue::TTestServer& server, ui64 tabletId, ETopicPartitionStatus status) {
    TPQTabletMock* mock = new TPQTabletMock(status);
    auto actorId = server.GetRuntime()->Register(mock);
    NKikimr::NTabletPipe::NTest::TPipeMock::Register(tabletId, actorId);
    return mock;
}


Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_Test) {
    NPersQueue::TTestServer server{};
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
    server.EnableLogs({NKikimrServices::PQ_PARTITION_CHOOSER}, NActors::NLog::PRI_TRACE);

    CreatePQTabletMock(server, 1000, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1001, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1002, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1003, ETopicPartitionStatus::Inactive);
    CreatePQTabletMock(server, 1004, ETopicPartitionStatus::Active);

    {
        auto r = ChoosePartition(server, SMEnabled, "A_Source");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    }   
    {
        auto r = ChoosePartition(server, SMEnabled, "Y_Source");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 2);
    }
    {
        // Define partition for sourceId that is not in partition boundary
        // We use this partition because it active
        WriteToTable(server, "X_Source_w_0", 0);
        auto r = ChoosePartition(server, SMEnabled, "X_Source_w_0");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    }
    {
        // Redefine partition  for sourceId. Check that partition changed;
        WriteToTable(server, "X_Source_w_0", 2);
        auto r = ChoosePartition(server, SMEnabled, "X_Source_w_0");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 2);
    }
    {
        // Redefine partition for sourceId to inactive partition. Select new partition use partition boundary.
        WriteToTable(server, "A_Source_w_0", 3);
        auto r = ChoosePartition(server, SMEnabled, "A_Source_w_0");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 4);
    }
    {
        // Use prefered partition, but sourceId not in partition boundary
        auto r = ChoosePartition(server, SMEnabled, "A_Source_1", 1);
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    }
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeDisabled_Test) {
    NPersQueue::TTestServer server{};
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
    server.EnableLogs({NKikimrServices::PQ_PARTITION_CHOOSER}, NActors::NLog::PRI_TRACE);

    CreatePQTabletMock(server, 1000, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1001, ETopicPartitionStatus::Active);
    CreatePQTabletMock(server, 1002, ETopicPartitionStatus::Active);

    {
        auto r = ChoosePartition(server, SMDisabled, "A_Source");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    }   
    {
        auto r = ChoosePartition(server, SMDisabled, "C_Source");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 2);
    }
    {
        WriteToTable(server, "A_Source_w_0", 0);
        auto r = ChoosePartition(server, SMDisabled, "A_Source_w_0");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    }
    {
        // Redefine partition  for sourceId. Check that partition changed;
        WriteToTable(server, "A_Source_w_0", 1);
        auto r = ChoosePartition(server, SMDisabled, "A_Source_w_0");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    }
    {
        // Redefine partition for sourceId to inactive partition. Select new partition.
        WriteToTable(server, "A_Source_w_0", 3);
        auto r = ChoosePartition(server, SMDisabled, "A_Source_w_0");
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    }
    {
        // Use prefered partition, and save it in table
        auto r = ChoosePartition(server, SMDisabled, "A_Source_1", 1);
        UNIT_ASSERT(r->Result);
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 1);
    }
}

}
