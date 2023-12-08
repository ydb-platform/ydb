#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/persqueue/writer/metadata_initializers.h>
#include <ydb/core/persqueue/writer/partition_chooser_impl.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>


using namespace NKikimr::NPQ;

static constexpr bool SMEnabled = true;
static constexpr bool SMDisabled = false;

NKikimrSchemeOp::TPersQueueGroupDescription CreateConfig(bool SplitMergeEnabled) {
    Cerr << ">>>>> SplitMergeEnabled=" << SplitMergeEnabled << Endl;
    NKikimrSchemeOp::TPersQueueGroupDescription result;
    NKikimrPQ::TPQTabletConfig* config =  result.MutablePQTabletConfig();

    auto* partitionStrategy = config->MutablePartitionStrategy();
    partitionStrategy->SetMinPartitionCount(3);
    partitionStrategy->SetMaxPartitionCount(SplitMergeEnabled ? 10 : 3);

    config->SetTopicName("/Root/topic-1");
    config->SetTopicPath("/Root");

    auto* p0 = result.AddPartitions();
    p0->SetPartitionId(0);
    p0->SetTabletId(1000);
    p0->MutableKeyRange()->SetToBound("C");

    auto* p1 = result.AddPartitions();
    p1->SetPartitionId(1);
    p1->SetTabletId(1001);
    p1->MutableKeyRange()->SetFromBound("C");
    p1->MutableKeyRange()->SetToBound("F");

    auto* p2 = result.AddPartitions();
    p2->SetPartitionId(2);
    p2->SetTabletId(1002);
    p2->MutableKeyRange()->SetFromBound("F");

    auto* p3 = result.AddPartitions();
    p3->SetStatus(::NKikimrPQ::ETopicPartitionStatus::Inactive);
    p3->SetPartitionId(3);
    p3->SetTabletId(1003);
    p3->MutableKeyRange()->SetFromBound("D");

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
    server.GetRuntime()->Register(NKikimr::NPQ::CreatePartitionChooserActor(parentId, 
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

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeEnabled_Test) {
    NPersQueue::TTestServer server{};

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
        WriteToTable(server, "X_Source_w_0", 0);
        auto r = ChoosePartition(server, SMEnabled, "X_Source_w_0");
        UNIT_ASSERT(r->Error);
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
        UNIT_ASSERT_VALUES_EQUAL(r->Result->Get()->PartitionId, 0);
    }
    {
        // Use prefered partition, but sourceId not in partition boundary
        auto r = ChoosePartition(server, SMEnabled, "A_Source_1", 1);
        UNIT_ASSERT(r->Error);
    }
}

Y_UNIT_TEST(TPartitionChooserActor_SplitMergeDisabled_Test) {
    NPersQueue::TTestServer server{};
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
    server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);

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
