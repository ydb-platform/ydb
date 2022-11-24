#include "columnshard_ut_common.h"
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>

namespace NKikimr {

using namespace NTxUT;
using NWrappers::NTestHelpers::TS3Mock;

namespace {

static const TVector<std::pair<TString, TTypeId>> testYdbSchema = TTestSchema::YdbSchema();

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

std::shared_ptr<arrow::TimestampArray> GetTimestampColumn(const TString& blob, const TString& srtSchema,
                                                          const std::string& columnName)
{
    auto schema = NArrow::DeserializeSchema(srtSchema);
    auto batch = NArrow::DeserializeBatch(blob, schema);
    UNIT_ASSERT(batch);

    std::shared_ptr<arrow::Array> array = batch->GetColumnByName(columnName);
    UNIT_ASSERT(array);
    return std::static_pointer_cast<arrow::TimestampArray>(array);
}

bool CheckSame(const TString& blob, const TString& srtSchema, ui32 expectedSize,
               const std::string& columnName, i64 seconds) {
    auto expected = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO));
    UNIT_ASSERT_VALUES_EQUAL(expected.value, seconds * 1000 * 1000);

    auto tsCol = GetTimestampColumn(blob, srtSchema, columnName);
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

std::vector<TString> MakeData(const std::vector<ui64>& ts, ui32 portionSize, ui32 overlapSize) {
    UNIT_ASSERT(ts.size() == 2);

    TString data1 = MakeTestBlob({0, portionSize}, testYdbSchema);
    UNIT_ASSERT(data1.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT);
    UNIT_ASSERT(data1.size() < 7 * 1024 * 1024);

    TString data2 = MakeTestBlob({overlapSize, overlapSize + portionSize}, testYdbSchema);
    UNIT_ASSERT(data2.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT);
    UNIT_ASSERT(data2.size() < 7 * 1024 * 1024);

    auto schema = NArrow::MakeArrowSchema(testYdbSchema);
    auto batch1 = UpdateColumn(NArrow::DeserializeBatch(data1, schema), TTestSchema::DefaultTtlColumn, ts[0]);
    auto batch2 = UpdateColumn(NArrow::DeserializeBatch(data2, schema), TTestSchema::DefaultTtlColumn, ts[1]);

    std::vector<TString> data;
    data.emplace_back(NArrow::SerializeBatchNoCompression(batch1));
    data.emplace_back(NArrow::SerializeBatchNoCompression(batch2));
    return data;
}

// ts[0] = 1600000000; // date -u --date='@1600000000' Sun Sep 13 12:26:40 UTC 2020
// ts[1] = 1620000000; // date -u --date='@1620000000' Mon May  3 00:00:00 UTC 2021
void TestTtl(bool reboots, bool internal, TTestSchema::TTableSpecials spec = {},
             std::vector<ui64> ts = {1600000000, 1620000000})
{
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime,
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard),
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

    UNIT_ASSERT(ts.size() == 2);

    ui32 ttlSec = TInstant::Now().Seconds(); // disable internal tll
    if (internal) {
        ttlSec -= (ts[0] + ts[1]) / 2; // enable internal ttl between ts1 and ts2
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

    ui32 portionSize = 80 * 1000;
    auto blobs = MakeData(ts, portionSize, portionSize / 2);
    UNIT_ASSERT_EQUAL(blobs.size(), 2);
    for (auto& data : blobs) {
        UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data));
        ProposeCommit(runtime, sender, metaShard, ++txId, {writeId});
        PlanCommit(runtime, sender, ++planStep, txId);
    }

    // TODO: write into path 2 (no ttl)

    if (reboots) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }

    if (internal) {
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {});
    } else {
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {tableId}, ts[0] + 1);
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
        UNIT_ASSERT(CheckSame(resRead.GetData(), schema, portionSize, TTestSchema::DefaultTtlColumn, ts[1]));
    }

    // Alter TTL
    ttlSec = TInstant::Now().Seconds() - (ts[1] + 1);
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
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {tableId}, ts[1] + 1);
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

    UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, blobs[0]));
    ProposeCommit(runtime, sender, metaShard, ++txId, {writeId});
    PlanCommit(runtime, sender, ++planStep, txId);

    if (internal) {
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {});
    } else {
        TriggerTTL(runtime, sender, {++planStep, ++txId}, {tableId}, ts[0] - 1);
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
        UNIT_ASSERT(CheckSame(resRead.GetData(), schema, portionSize, TTestSchema::DefaultTtlColumn, ts[0]));
    }
}

std::vector<std::pair<std::shared_ptr<arrow::TimestampArray>, ui64>>
TestTiers(bool reboots, const std::vector<TString>& blobs, const std::vector<TTestSchema::TTableSpecials>& specs) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    //runtime.SetLogPriority(NKikimrServices::BLOB_CACHE, NActors::NLog::PRI_DEBUG);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime,
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard),
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

    UNIT_ASSERT(specs.size() > 0);
    bool ok = ProposeSchemaTx(runtime, sender,
                              TTestSchema::CreateTableTxBody(tableId, testYdbSchema, specs[0]),
                              {++planStep, ++txId});
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, {planStep, txId});

    for (auto& data : blobs) {
        UNIT_ASSERT(WriteData(runtime, sender, metaShard, ++writeId, tableId, data));
        ProposeCommit(runtime, sender, metaShard, ++txId, {writeId});
        PlanCommit(runtime, sender, ++planStep, txId);
    }

    if (reboots) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }

    TAutoPtr<IEventHandle> handle;

    std::vector<std::pair<std::shared_ptr<arrow::TimestampArray>, ui64>> resColumns;
    resColumns.reserve(specs.size());

    for (ui32 i = 0; i < specs.size(); ++i) {
        if (i) {
            ui32 version = i + 1;
            ok = ProposeSchemaTx(runtime, sender,
                                 TTestSchema::AlterTableTxBody(tableId, version, specs[i]),
                                 {++planStep, ++txId});
            UNIT_ASSERT(ok);
            PlanSchemaTx(runtime, sender, {planStep, txId});

            if (reboots) {
                RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            }
        }

        TriggerTTL(runtime, sender, {++planStep, ++txId}, {});
#if 0
        if (i) {
            sleep(10); // TODO: wait export
        }
#endif
        // Read

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

        if (resRead.GetData().size()) {
            auto& meta = resRead.GetMeta();
            auto& schema = meta.GetSchema();
            auto tsColumn = GetTimestampColumn(resRead.GetData(), schema, TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT(tsColumn);

            UNIT_ASSERT(meta.HasReadStats());
            auto& readStats = meta.GetReadStats();
            ui64 numBytes = readStats.GetDataBytes(); // compressed bytes in storage

            resColumns.emplace_back(tsColumn, numBytes);
        } else {
            resColumns.emplace_back(nullptr, 0);
        }

        if (reboots) {
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
    }

    return resColumns;
}

void TestTwoTiers(const TTestSchema::TTableSpecials& spec, bool compressed, bool reboots) {
    std::vector<ui64> ts = {1600000000, 1620000000};
    ui64 nowSec = TInstant::Now().Seconds();

    std::vector<TTestSchema::TTableSpecials> alters(4, spec);

    ui64 allowBoth = nowSec - ts[0] + 600;
    ui64 allowOne = nowSec - ts[1] + 600;
    ui64 allowNone = nowSec - ts[1] - 600;

    alters[0].Tiers[0].SetTtl(allowBoth); // tier0 allows/has: data[0], data[1]
    alters[0].Tiers[1].SetTtl(allowBoth); // tier1 allows: data[0], data[1], has: nothing

    alters[1].Tiers[0].SetTtl(allowOne); // tier0 allows/has: data[1]
    alters[1].Tiers[1].SetTtl(allowBoth); // tier1 allows: data[0], data[1], has: data[0]

    alters[2].Tiers[0].SetTtl(allowNone); // tier0 allows/has: nothing
    alters[2].Tiers[1].SetTtl(allowOne); // tier1 allows/has: data[1]

    alters[3].Tiers[0].SetTtl(allowNone); // tier0 allows/has: nothing
    alters[3].Tiers[1].SetTtl(allowNone); // tier1 allows/has: nothing

    ui32 portionSize = 80 * 1000;
    ui32 overlapSize = 40 * 1000;
    std::vector<TString> blobs = MakeData(ts, portionSize, overlapSize);

    auto columns = TestTiers(reboots, blobs, alters);

    UNIT_ASSERT_EQUAL(columns.size(), 4);
    UNIT_ASSERT(columns[0].first);
    UNIT_ASSERT(columns[1].first);
    UNIT_ASSERT(columns[2].first);
    UNIT_ASSERT(!columns[3].first);
    UNIT_ASSERT(columns[0].second);
    UNIT_ASSERT(columns[1].second);
    UNIT_ASSERT(columns[2].second);
    UNIT_ASSERT(!columns[3].second);

    UNIT_ASSERT_EQUAL(columns[0].first->length(), 2 * portionSize - overlapSize);
    UNIT_ASSERT_EQUAL(columns[0].first->length(), columns[1].first->length());
    UNIT_ASSERT_EQUAL(columns[2].first->length(), portionSize);

    Cerr << "read bytes: " << columns[0].second << ", " << columns[1].second << ", " << columns[2].second << "\n";
    if (compressed) {
        UNIT_ASSERT_GT(columns[0].second, columns[1].second);
    } else {
        UNIT_ASSERT_EQUAL(columns[0].second, columns[1].second);
    }
}

void TestTwoHotTiers(bool reboot) {
    TTestSchema::TTableSpecials spec;
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier0"));
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier1"));
    spec.Tiers.back().SetCodec("zstd");

    TestTwoTiers(spec, true, reboot);
}

#if 0
void TestHotAndColdTiers(bool reboot) {
#if 1
    TString bucket = "ydb";
    TPortManager portManager;
    ui16 port = portManager.GetPort();
    TString connString = Sprintf("localhost:%hu", port);
    Cerr << "S3 at " << connString << "\n";

    TS3Mock s3Mock({}, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());
#endif

    TTestSchema::TTableSpecials spec;
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier0"));
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier1"));
    spec.Tiers.back().S3 = NKikimrSchemeOp::TS3Settings();
    auto& s3 = *spec.Tiers.back().S3;

    s3.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
    s3.SetVerifySSL(false);
#if 0
    s3.SetEndpoint("storage.cloud-preprod.yandex.net");
    s3.SetBucket("ch-s3");
    s3.SetAccessKey(); <--
    s3.SetSecretKey(); <--
    s3.SetProxyHost("localhost");
    s3.SetProxyPort(8080);
    s3.SetProxyScheme(NKikimrSchemeOp::TS3Settings::HTTP);
#else
    s3.SetEndpoint(connString);
    s3.SetBucket(bucket);
#endif

    TestTwoTiers(spec, false, reboot);
}
#endif

void TestDrop(bool reboots) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime,
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard),
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

void TestDropWriteRace() {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime,
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard),
                           &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    //

    ui64 tableId = 1;
    ui64 planStep = 1000000000; // greater then delays
    ui64 txId = 100;

    NLongTxService::TLongTxId longTxId;
    UNIT_ASSERT(longTxId.ParseString("ydb://long-tx/01ezvvxjdk2hd4vdgjs68knvp8?node_id=1"));

    bool ok = ProposeSchemaTx(runtime, sender, TTestSchema::CreateTableTxBody(tableId, testYdbSchema),
                              {++planStep, ++txId});
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, {planStep, txId});

    TString data = MakeTestBlob({0, 100}, testYdbSchema);
    UNIT_ASSERT(data.size() < NColumnShard::TLimits::MIN_BYTES_TO_INSERT);

    // Write into InsertTable
    auto writeIdOpt = WriteData(runtime, sender, longTxId, tableId, "0", data);
    UNIT_ASSERT(writeIdOpt);
    ProposeCommit(runtime, sender, ++txId, {*writeIdOpt});
    auto commitTxId = txId;

    // Drop table
    ok = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(tableId, 2), {++planStep, ++txId});
    if (ok) {
        PlanSchemaTx(runtime, sender, {planStep, txId});
    }

    // Plan commit
    PlanCommit(runtime, sender, ++planStep, commitTxId);
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

    Y_UNIT_TEST(HotTiers) {
        TestTwoHotTiers(false);
    }

    Y_UNIT_TEST(RebootHotTiers) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestTwoHotTiers(true);
    }

    Y_UNIT_TEST(ColdTiers) {
        // Disabled KIKIMR-14942
        //TestHotAndColdTiers(false);
    }

    Y_UNIT_TEST(RebootColdTiers) {
        // Disabled KIKIMR-14942
        //NColumnShard::gAllowLogBatchingDefaultValue = false;
        //TestHotAndColdTiers(true);
    }

    Y_UNIT_TEST(Drop) {
        TestDrop(false);
    }

    Y_UNIT_TEST(RebootDrop) {
        TestDrop(true);
    }

    Y_UNIT_TEST(DropWriteRace) {
        TestDropWriteRace();
    }
}

}
