#include "columnshard_ut_common.h" 
 
namespace NKikimr { 
 
using namespace NTxUT; 
 
namespace { 
 
static const TVector<std::pair<TString, TTypeId>> testYdbSchema = TTestSchema::YdbSchema(); 
//static const TVector<std::pair<TString, TTypeId>> testYdbPkSchema = TTestSchema::YdbPkSchema(); 
 
std::shared_ptr<arrow::RecordBatch> UpdateColumn(std::shared_ptr<arrow::RecordBatch> batch, TString columnName, i64 seconds) { 
    std::string name(columnName.c_str(), columnName.size()); 
 
    auto schema = batch->schema(); 
    int pos = schema->GetFieldIndex(name); 
    UNIT_ASSERT(pos > 0); 
    UNIT_ASSERT(batch->GetColumnByName(name)->type_id() == arrow::Type::TIMESTAMP); 
 
    auto scalar = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO)); 
    UNIT_ASSERT_VALUES_EQUAL(scalar.value, seconds * 1000 * 1000); 
 
    auto res = arrow::MakeArrayFromScalar(scalar, batch->num_rows()); 
    UNIT_ASSERT(res.ok()); 
 
    auto columns = batch->columns(); 
    columns[pos] = *res; 
    return arrow::RecordBatch::Make(schema, batch->num_rows(), columns); 
} 
 
bool TriggerTTL(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap, const TVector<ui64>& pathIds, 
                ui64 tsSeconds = 0, const TString& ttlColumnName = TTestSchema::DefaultTtlColumn) { 
    TString txBody = TTestSchema::TtlTxBody(pathIds, ttlColumnName, tsSeconds); 
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>( 
        NKikimrTxColumnShard::TX_KIND_TTL, sender, snap.TxId, txBody); 
 
    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender); 
    const auto& res = ev->Get()->Record; 
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.TxId); 
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_TTL); 
    return (res.GetStatus() == NKikimrTxColumnShard::SUCCESS); 
} 
 
bool CheckSame(const TString& blob, const TString& srtSchema, ui32 expectedSize, 
               const std::string& columnName, i64 seconds) { 
    auto schema = NArrow::DeserializeSchema(srtSchema); 
    auto batch = NArrow::DeserializeBatch(blob, schema); 
    UNIT_ASSERT(batch); 
 
    auto expected = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO)); 
    UNIT_ASSERT_VALUES_EQUAL(expected.value, seconds * 1000 * 1000); 
 
    std::shared_ptr<arrow::Array> array = batch->GetColumnByName(columnName); 
    UNIT_ASSERT(array); 
    auto tsCol = std::static_pointer_cast<arrow::TimestampArray>(array); 
    UNIT_ASSERT(tsCol); 
    UNIT_ASSERT_VALUES_EQUAL(tsCol->length(), expectedSize); 
 
    for (int i = 0; i < tsCol->length(); ++i) { 
        i64 value = tsCol->Value(i); 
        if (value != expected.value) { 
            Cerr << "Unexpected: " << value << ", expected " << expected.value << "\n"; 
            return false; 
        } 
    } 
    return true; 
} 
 
void TestTtl(bool reboots, bool internal, TTestSchema::TTableSpecials spec = {}) { 
    TTestBasicRuntime runtime; 
    TTester::Setup(runtime); 
 
    TActorId sender = runtime.AllocateEdgeActor(); 
    CreateTestBootstrapper(runtime, 
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::COLUMNSHARD), 
                           &CreateColumnShard); 
 
    TDispatchOptions options; 
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot)); 
    runtime.DispatchEvents(options); 
 
    // 
 
    ui64 metaShard = TTestTxConfig::TxTablet1;
    ui64 writeId = 0; 
    ui64 tableId = 1; 
    ui64 planStep = 1000000000; // greater then delays 
    ui64 txId = 100; 
 
    ui64 ts1 = 1600000000; // date -u --date='@1600000000' Sun Sep 13 12:26:40 UTC 2020 
    ui64 ts2 = 1620000000; // date -u --date='@1620000000' Mon May  3 00:00:00 UTC 2021 
 
    ui32 ttlSec = TInstant::Now().Seconds(); // disable internal tll 
    if (internal) { 
        ttlSec -= (ts1 + ts2) / 2; // enable internal ttl between ts1 and ts2 
    } 
    if (spec.HasTiers()) { 
        spec.Tiers[0].SetTtl(ttlSec); 
    } else { 
        spec.SetTtl(ttlSec); 
    } 
    bool ok = ProposeSchemaTx(runtime, sender, 
                              TTestSchema::CreateTableTxBody(tableId, testYdbSchema, spec), 
                              {++planStep, ++txId}); 
    UNIT_ASSERT(ok); 
    PlanSchemaTx(runtime, sender, {planStep, txId}); 
 
    // 
 
    static const ui32 portionSize = 80 * 1000; 
    static const ui32 overlapSize = 40 * 1000; 
 
    TString data1 = MakeTestBlob({0, portionSize}, testYdbSchema); 
    UNIT_ASSERT(data1.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT); 
    UNIT_ASSERT(data1.size() < 7 * 1024 * 1024); 
 
    TString data2 = MakeTestBlob({overlapSize, overlapSize + portionSize}, testYdbSchema); 
    UNIT_ASSERT(data2.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT); 
    UNIT_ASSERT(data2.size() < 7 * 1024 * 1024); 
 
    auto schema = NArrow::MakeArrowSchema(testYdbSchema); 
    auto batch1 = NArrow::DeserializeBatch(data1, schema); 
    auto batch2 = NArrow::DeserializeBatch(data2, schema); 
 
    data1 = NArrow::SerializeBatchNoCompression(UpdateColumn(batch1, TTestSchema::DefaultTtlColumn, ts1)); 
    data2 = NArrow::SerializeBatchNoCompression(UpdateColumn(batch2, TTestSchema::DefaultTtlColumn, ts2)); 
 
    UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data1)); 
    ProposeCommit(runtime, sender, metaShard, ++txId, {writeId}); 
    PlanCommit(runtime, sender, ++planStep, txId); 
 
    UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data2)); 
    ProposeCommit(runtime, sender, metaShard, ++txId, {writeId}); 
    PlanCommit(runtime, sender, ++planStep, txId); 
 
    // TODO: write into path 2 (no ttl) 
 
    if (reboots) { 
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    } 
 
    if (internal) { 
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {}); 
    } else { 
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {tableId}, ts1 + 1); 
    } 
 
    TAutoPtr<IEventHandle> handle; 
 
    if (reboots) { 
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    } 
 
    { 
        --planStep; 
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId); 
        Proto(read.get()).AddColumnNames(TTestSchema::DefaultTtlColumn); 
 
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle); 
        UNIT_ASSERT(event); 
 
        auto& resRead = Proto(event); 
        UNIT_ASSERT_EQUAL(resRead.GetOrigin(), TTestTxConfig::TxTablet0);
        UNIT_ASSERT_EQUAL(resRead.GetTxInitiator(), metaShard); 
        UNIT_ASSERT_EQUAL(resRead.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS); 
        UNIT_ASSERT_EQUAL(resRead.GetBatch(), 0); 
        UNIT_ASSERT_EQUAL(resRead.GetFinished(), true); 
        UNIT_ASSERT(resRead.GetData().size() > 0); 
 
        auto& schema = resRead.GetMeta().GetSchema(); 
        UNIT_ASSERT(CheckSame(resRead.GetData(), schema, portionSize, TTestSchema::DefaultTtlColumn, ts2)); 
    } 
 
    // Alter TTL 
    ttlSec = TInstant::Now().Seconds() - (ts2 + 1); 
    if (spec.HasTiers()) { 
        spec.Tiers[0].SetTtl(ttlSec); 
    } else { 
        spec.SetTtl(ttlSec); 
    } 
    ok = ProposeSchemaTx(runtime, sender, 
                         TTestSchema::AlterTableTxBody(tableId, 2, spec), 
                         {++planStep, ++txId}); 
    UNIT_ASSERT(ok); 
    PlanSchemaTx(runtime, sender, {planStep, txId}); 
 
    if (internal) { 
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {}); 
    } else { 
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {tableId}, ts2 + 1); 
    } 
 
    { 
        --planStep; 
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId); 
        Proto(read.get()).AddColumnNames(TTestSchema::DefaultTtlColumn); 
 
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle); 
        UNIT_ASSERT(event); 
 
        auto& resRead = Proto(event); 
        UNIT_ASSERT_EQUAL(resRead.GetOrigin(), TTestTxConfig::TxTablet0);
        UNIT_ASSERT_EQUAL(resRead.GetTxInitiator(), metaShard); 
        UNIT_ASSERT_EQUAL(resRead.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS); 
        UNIT_ASSERT_EQUAL(resRead.GetBatch(), 0); 
        UNIT_ASSERT_EQUAL(resRead.GetFinished(), true); 
        UNIT_ASSERT_EQUAL(resRead.GetData().size(), 0); 
    } 
 
    // Disable TTL 
    ok = ProposeSchemaTx(runtime, sender, 
                         TTestSchema::AlterTableTxBody(tableId, 3, TTestSchema::TTableSpecials()), 
                         {++planStep, ++txId}); 
    UNIT_ASSERT(ok); 
    PlanSchemaTx(runtime, sender, {planStep, txId}); 
 
    UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data1)); 
    ProposeCommit(runtime, sender, metaShard, ++txId, {writeId}); 
    PlanCommit(runtime, sender, ++planStep, txId); 
 
    if (internal) { 
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {}); 
    } else { 
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {tableId}, ts1 - 1); 
    } 
 
    { 
        --planStep; 
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId); 
        Proto(read.get()).AddColumnNames(TTestSchema::DefaultTtlColumn); 
 
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle); 
        UNIT_ASSERT(event); 
 
        auto& resRead = Proto(event); 
        UNIT_ASSERT_EQUAL(resRead.GetOrigin(), TTestTxConfig::TxTablet0);
        UNIT_ASSERT_EQUAL(resRead.GetTxInitiator(), metaShard); 
        UNIT_ASSERT_EQUAL(resRead.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS); 
        UNIT_ASSERT_EQUAL(resRead.GetBatch(), 0); 
        UNIT_ASSERT_EQUAL(resRead.GetFinished(), true); 
        UNIT_ASSERT(resRead.GetData().size() > 0); 
 
        auto& schema = resRead.GetMeta().GetSchema(); 
        UNIT_ASSERT(CheckSame(resRead.GetData(), schema, portionSize, TTestSchema::DefaultTtlColumn, ts1)); 
    } 
} 
 
void TestDrop(bool reboots) { 
    TTestBasicRuntime runtime; 
    TTester::Setup(runtime); 
 
    TActorId sender = runtime.AllocateEdgeActor(); 
    CreateTestBootstrapper(runtime, 
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::COLUMNSHARD), 
                           &CreateColumnShard); 
 
    TDispatchOptions options; 
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot)); 
    runtime.DispatchEvents(options); 
 
    // 
 
    ui64 metaShard = TTestTxConfig::TxTablet1;
    ui64 writeId = 0; 
    ui64 tableId = 1; 
    ui64 planStep = 1000000000; // greater then delays 
    ui64 txId = 100; 
 
    bool ok = ProposeSchemaTx(runtime, sender, TTestSchema::CreateTableTxBody(tableId, testYdbSchema), 
                              {++planStep, ++txId}); 
    UNIT_ASSERT(ok); 
    PlanSchemaTx(runtime, sender, {planStep, txId}); 
 
    // 
 
    static const ui32 portionSize = 80 * 1000; 
 
    TString data1 = MakeTestBlob({0, portionSize}, testYdbSchema); 
    UNIT_ASSERT(data1.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT); 
    UNIT_ASSERT(data1.size() < 7 * 1024 * 1024); 
 
    TString data2 = MakeTestBlob({0, 100}, testYdbSchema); 
    UNIT_ASSERT(data2.size() < NColumnShard::TLimits::MIN_BYTES_TO_INSERT); 
 
    // Write into index 
    UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data1)); 
    ProposeCommit(runtime, sender, metaShard, ++txId, {writeId}); 
    PlanCommit(runtime, sender, ++planStep, txId); 
 
    // Write into InsertTable 
    UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data2)); 
    ProposeCommit(runtime, sender, metaShard, ++txId, {writeId}); 
    PlanCommit(runtime, sender, ++planStep, txId); 
 
    if (reboots) { 
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    } 
 
    // Drop table 
    ok = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(tableId, 2), {++planStep, ++txId}); 
    UNIT_ASSERT(ok); 
    PlanSchemaTx(runtime, sender, {planStep, txId}); 
 
    if (reboots) { 
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    } 
 
    TAutoPtr<IEventHandle> handle; 
    { 
        --planStep; 
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId); 
        Proto(read.get()).AddColumnNames(TTestSchema::DefaultTtlColumn); 
 
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle); 
        UNIT_ASSERT(event); 
 
        auto& resRead = Proto(event); 
        UNIT_ASSERT_EQUAL(resRead.GetOrigin(), TTestTxConfig::TxTablet0);
        UNIT_ASSERT_EQUAL(resRead.GetTxInitiator(), metaShard); 
        UNIT_ASSERT_EQUAL(resRead.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS); 
        UNIT_ASSERT_EQUAL(resRead.GetBatch(), 0); 
        UNIT_ASSERT_EQUAL(resRead.GetFinished(), true); 
        UNIT_ASSERT_EQUAL(resRead.GetData().size(), 0); 
    } 
} 
 
} 
 
namespace NColumnShard {
extern bool gAllowLogBatchingDefaultValue;
}
 
Y_UNIT_TEST_SUITE(TColumnShardTestSchema) { 
    Y_UNIT_TEST(ExternalTTL) { 
        TestTtl(false, false); 
    } 
 
    Y_UNIT_TEST(RebootExternalTTL) { 
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestTtl(true, false); 
    } 
 
    Y_UNIT_TEST(InternalTTL) { 
        TestTtl(false, true); 
    } 
 
    Y_UNIT_TEST(RebootInternalTTL) { 
        NColumnShard::gAllowLogBatchingDefaultValue = false; 
        TestTtl(true, true); 
    } 
 
    Y_UNIT_TEST(OneTier) { 
        TTestSchema::TTableSpecials specs; 
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default")); 
        TestTtl(false, true, specs); 
    } 
 
    Y_UNIT_TEST(RebootOneTier) { 
        NColumnShard::gAllowLogBatchingDefaultValue = false; 
        TTestSchema::TTableSpecials specs; 
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default")); 
        TestTtl(true, true, specs); 
    } 
 
    Y_UNIT_TEST(OneTierExternalTtl) { 
        TTestSchema::TTableSpecials specs; 
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default")); 
        TestTtl(false, false, specs); 
    } 
 
    Y_UNIT_TEST(RebootOneTierExternalTtl) { 
        NColumnShard::gAllowLogBatchingDefaultValue = false; 
        TTestSchema::TTableSpecials specs; 
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default")); 
        TestTtl(true, false, specs); 
    } 
 
    Y_UNIT_TEST(Drop) { 
        TestDrop(false); 
    } 
 
    Y_UNIT_TEST(RebootDrop) { 
        TestDrop(true); 
    } 
} 
 
} 
