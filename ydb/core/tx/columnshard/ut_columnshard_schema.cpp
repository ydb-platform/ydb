#include "columnshard_ut_common.h"
#include "external_data.h"
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/services/metadata/service.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
//#include <ydb/core/testlib/cs_helper.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/actors/core/av_bootstrapped.h>

#include <util/system/hostname.h>

namespace NKikimr {

using namespace NTxUT;
using namespace NColumnShard;
using NWrappers::NTestHelpers::TS3Mock;

namespace {

static const TVector<std::pair<TString, TTypeInfo>> testYdbSchema = TTestSchema::YdbSchema();
static const TVector<std::pair<TString, TTypeInfo>> testYdbPk = TTestSchema::YdbPkSchema();

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

std::shared_ptr<arrow::Array> GetFirstPKColumn(const TString& blob, const TString& srtSchema,
                                               const std::string& columnName)
{
    auto schema = NArrow::DeserializeSchema(srtSchema);
    auto batch = NArrow::DeserializeBatch(blob, schema);
    UNIT_ASSERT(batch);

    std::shared_ptr<arrow::Array> array = batch->GetColumnByName(columnName);
    UNIT_ASSERT(array);
    return array;
}

bool CheckSame(const TString& blob, const TString& srtSchema, ui32 expectedSize,
               const std::string& columnName, i64 seconds) {
    auto expected = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO));
    UNIT_ASSERT_VALUES_EQUAL(expected.value, seconds * 1000 * 1000);

    auto tsCol = GetFirstPKColumn(blob, srtSchema, columnName);
    UNIT_ASSERT(tsCol);
    UNIT_ASSERT_VALUES_EQUAL(tsCol->length(), expectedSize);

    for (int i = 0; i < tsCol->length(); ++i) {
        auto value = *tsCol->GetScalar(i);
        if (!value->Equals(expected)) {
            Cerr << "Unexpected: '" << value->ToString() << "', expected " << expected.value << "\n";
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

bool TestCreateTable(const TString& txBody, ui64 planStep = 1000, ui64 txId = 100) {
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
    return ProposeSchemaTx(runtime, sender, txBody, {++planStep, ++txId});
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
                              TTestSchema::CreateTableTxBody(tableId, testYdbSchema, testYdbPk, spec),
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

class TCountersContainer {
private:
    ui32 RestartTabletOnPutData = 0;
    ui32 SuccessCounterStart = 0;
public:
    ui32 UnknownsCounter = 0;
    ui32 SuccessCounter = 0;
    ui32 ErrorsCounter = 0;
    ui32 ResponsesCounter = 0;

    TCountersContainer& SetRestartTabletOnPutData(const ui32 value) {
        RestartTabletOnPutData = value;
        return *this;
    }

    bool PopRestartTabletOnPutData() {
        if (!RestartTabletOnPutData) {
            return false;
        }
        --RestartTabletOnPutData;
        return true;
    }
    TString SerializeToString() const {
        TStringBuilder sb;
        sb << "EXPORTS INFO: " << SuccessCounter << "/" << ErrorsCounter << "/" << UnknownsCounter << "/" << ResponsesCounter;
        return sb;
    }

    void WaitEvents(TTestBasicRuntime& runtime, const TActorId sender, const ui32 attemption, const ui32 expectedDeltaSuccess, const TDuration timeout) {
        const TInstant startInstant = Now();
        const TInstant deadline = startInstant + timeout;
        Cerr << "START_WAITING(" << attemption << "): " << SerializeToString() << Endl;
        while (Now() < deadline) {
            if (PopRestartTabletOnPutData()) {
                RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
            }
            Cerr << "IN_WAITING(" << attemption << "):" << SerializeToString() << Endl;
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(5));
            UNIT_ASSERT(ErrorsCounter == 0);
            if (expectedDeltaSuccess) {
                if (SuccessCounter >= SuccessCounterStart + expectedDeltaSuccess) {
                    break;
                }
            } else {
                if (SuccessCounter > SuccessCounterStart) {
                    break;
                }
            }
        }
        if (expectedDeltaSuccess) {
            UNIT_ASSERT(SuccessCounter >= SuccessCounterStart + expectedDeltaSuccess);
        } else {
            UNIT_ASSERT(SuccessCounter == SuccessCounterStart);
        }
        Cerr << "FINISH_WAITING(" << attemption << "): " << SerializeToString() << Endl;
        SuccessCounterStart = SuccessCounter;
    }
};

class TEventsCounter {
private:
    TCountersContainer* Counters = nullptr;
    TTestBasicRuntime& Runtime;
    const TActorId Sender;
private:
    template <class TPrivateEvent>
    static TPrivateEvent* TryGetPrivateEvent(TAutoPtr<IEventHandle>& ev) {
        if (ev->GetTypeRewrite() != TPrivateEvent::EventType) {
            return nullptr;
        }
        return dynamic_cast<TPrivateEvent*>(ev->GetBase());
    }
public:

    TEventsCounter(TCountersContainer& counters, TTestBasicRuntime& runtime, const TActorId sender)
        : Counters(&counters)
        , Runtime(runtime)
        , Sender(sender)
    {
        Y_UNUSED(Runtime);
        Y_UNUSED(Sender);
    }
    bool operator()(TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        TStringBuilder ss;
        if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvExport>(ev)) {
            ss << "EXPORT";
            if (msg->Status == NKikimrProto::OK) {
                ss << "(" << ++Counters->SuccessCounter << "): SUCCESS";
            }
            if (msg->Status == NKikimrProto::ERROR) {
                ss << "(" << ++Counters->ErrorsCounter << "): ERROR";
            }
            if (msg->Status == NKikimrProto::UNKNOWN) {
                ss << "(" << ++Counters->UnknownsCounter << "): UNKNOWN";
            }
        } else if (auto* msg = TryGetPrivateEvent<NWrappers::NExternalStorage::TEvPutObjectResponse>(ev)) {
            ss << "S3_RESPONSE(" << ++Counters->ResponsesCounter << "):";
        } else {
            return false;
        }
        ss << " " << ev->Sender << "->" << ev->Recipient;
        Cerr << ss << Endl;
        return false;
    };
};

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
                              TTestSchema::CreateTableTxBody(tableId, testYdbSchema, testYdbPk, specs[0]),
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

    TCountersContainer counter;
    runtime.SetEventFilter(TEventsCounter(counter, runtime, sender));
    for (ui32 i = 0; i < specs.size(); ++i) {
        if (i) {
            ui32 version = i + 1;
            ok = ProposeSchemaTx(runtime, sender,
                                 TTestSchema::AlterTableTxBody(tableId, version, specs[i]),
                                 {++planStep, ++txId});
            UNIT_ASSERT(ok);
            PlanSchemaTx(runtime, sender, {planStep, txId});

        }
        bool hasEvictionSettings = false;
        for (auto&& i : specs[i].Tiers) {
            if (!!i.S3) {
                hasEvictionSettings = true;
                break;
            }
        }

        counter.SetRestartTabletOnPutData(reboots ? 1 : 0);

        TriggerTTL(runtime, sender, {++planStep, ++txId}, {});
        if (hasEvictionSettings) {
            if (i == 1 || i == 2) {
                counter.WaitEvents(runtime, sender, i, 1, TDuration::Seconds(40));
            } else {
                counter.WaitEvents(runtime, sender, i, 0, TDuration::Seconds(20));
            }
        } else {
            counter.WaitEvents(runtime, sender, i, 0, TDuration::Seconds(5));
        }

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
            auto pkColumn = GetFirstPKColumn(resRead.GetData(), schema, TTestSchema::DefaultTtlColumn);
            UNIT_ASSERT(pkColumn);
            UNIT_ASSERT(pkColumn->type_id() == arrow::Type::TIMESTAMP);

            UNIT_ASSERT(meta.HasReadStats());
            auto& readStats = meta.GetReadStats();
            ui64 numBytes = readStats.GetDataBytes(); // compressed bytes in storage

            auto tsColumn = std::static_pointer_cast<arrow::TimestampArray>(pkColumn);
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

void TestHotAndColdTiers(bool reboot) {
    TString bucket = "ydb";
    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock({}, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    TTestSchema::TTableSpecials spec;
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier0"));
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier1"));
    spec.Tiers.back().S3 = NKikimrSchemeOp::TS3Settings();
    auto& s3Config = *spec.Tiers.back().S3;
    {

        s3Config.SetScheme(NKikimrSchemeOp::TS3Settings::HTTP);
        s3Config.SetVerifySSL(false);
#if 0
        s3Config.SetEndpoint("storage.cloud-preprod.yandex.net");
        s3Config.SetBucket("tiering-test-01");
        s3Config.SetAccessKey("...");
        s3Config.SetSecretKey("...");
        s3Config.SetProxyHost("localhost");
        s3Config.SetProxyPort(8080);
        s3Config.SetProxyScheme(NKikimrSchemeOp::TS3Settings::HTTP);
#else
        s3Config.SetEndpoint("fake");
        s3Config.SetBucket(bucket);
#endif
        s3Config.SetRequestTimeoutMs(10000);
        s3Config.SetHttpRequestTimeoutMs(10000);
        s3Config.SetConnectionTimeoutMs(10000);
    }

    TestTwoTiers(spec, false, reboot);
}

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

    bool ok = ProposeSchemaTx(runtime, sender, TTestSchema::CreateTableTxBody(tableId, testYdbSchema, testYdbPk),
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
/*
class TLocalHelper: public Tests::NCS::THelper {
private:
    using TBase = Tests::NCS::THelper;
public:
    using TBase::TBase;
    void CreateTestOlapTable(TString tableName = "olapTable", TString storeName = "olapStore",
        ui32 storeShardsCount = 4, ui32 tableShardsCount = 3,
        TString shardingFunction = "HASH_FUNCTION_CLOUD_LOGS") {
        TActorId sender = Server.GetRuntime()->AllocateEdgeActor();
        CreateTestOlapStore(sender, Sprintf(R"(
             Name: "%s"
             ColumnShardCount: %d
             SchemaPresets {
                 Name: "default"
                 Schema {
                     Columns { Name: "timestamp" Type: "Timestamp" }
                     #Columns { Name: "resource_type" Type: "Utf8" }
                     Columns { Name: "resource_id" Type: "Utf8" }
                     Columns { Name: "uid" Type: "Utf8" }
                     Columns { Name: "level" Type: "Int32" }
                     Columns { Name: "message" Type: "Utf8" }
                     #Columns { Name: "json_payload" Type: "Json" }
                     #Columns { Name: "ingested_at" Type: "Timestamp" }
                     #Columns { Name: "saved_at" Type: "Timestamp" }
                     #Columns { Name: "request_id" Type: "Utf8" }
                     KeyColumnNames: "timestamp"
                     Engine: COLUMN_ENGINE_REPLACING_TIMESERIES
                     StorageTiers {
                         Name: "tier1"
                         ObjectStorage {
                             Endpoint: "fake"
                             AccessKey: "$a"
                         }
                     }
                 }
             }
        )", storeName.c_str(), storeShardsCount));

        TString shardingColumns = "[\"timestamp\", \"uid\"]";
        if (shardingFunction != "HASH_FUNCTION_CLOUD_LOGS") {
            shardingColumns = "[\"uid\"]";
        }

        TBase::CreateTestOlapTable(sender, storeName, Sprintf(R"(
            Name: "%s"
            ColumnShardCount: %d
            Sharding {
                HashSharding {
                    Function: %s
                    Columns: %s
                }
            })", tableName.c_str(), tableShardsCount, shardingFunction.c_str(), shardingColumns.c_str()));
    }
};
*/

Y_UNIT_TEST_SUITE(TColumnShardTestSchema) {
    class TTestCSEmulator: public NActors::TActorBootstrapped<TTestCSEmulator> {
    private:
        using TBase = NActors::TActorBootstrapped<TTestCSEmulator>;
        std::shared_ptr<TSnapshotConstructor> ExternalDataManipulation;
        TActorId ProviderId;
        TInstant Start;
        YDB_READONLY_FLAG(Found, false);
    public:
        STFUNC(StateInit) {
            switch (ev->GetTypeRewrite()) {
                HFunc(NMetadataProvider::TEvRefreshSubscriberData, Handle);
                default:
                    Y_VERIFY(false);
            }
        }

        void Handle(NMetadataProvider::TEvRefreshSubscriberData::TPtr& ev, const TActorContext&) {
            auto value = ev->Get()->GetSnapshotAs<TCSKVSnapshot>()->GetValue("/Root/olapStore.tier1.a");
            if (value && *value == "b") {
                FoundFlag = true;
            } else {
                Cerr << ev->Get()->GetSnapshot()->SerializeToString() << Endl;
            }
        }

        void Bootstrap() {
            ProviderId = NMetadataProvider::MakeServiceId(1);
            ExternalDataManipulation = std::make_shared<TSnapshotConstructor>("/Root/.external_data/kv_settings");
            Become(&TThis::StateInit);
            Sender<NMetadataProvider::TEvSubscribeExternal>(ExternalDataManipulation).SendTo(ProviderId);
            Start = Now();
        }
    };

    Y_UNIT_TEST(DSConfigsStub) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableKqpSessionActor(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
        //        server->SetupDefaultProfiles();

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->SetupRootStoragePools(sender);
        {
            TTestCSEmulator* emulator = new TTestCSEmulator;
            runtime.Register(emulator);
            {
                const TInstant start = Now();
                while (Now() - start < TDuration::Seconds(10)) {
                    runtime.WaitForEdgeEvents([](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                        Cerr << "Step " << event->Type << Endl;
                        return false;
                        }, {}, TDuration::Seconds(1));
                    Sleep(TDuration::Seconds(1));
                    Cerr << "Step finished" << Endl;
                }
            }

            NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
            tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
                auto sResult = f.GetValueSync();
                Cerr << sResult.GetIssues().ToString() << Endl;
                auto session = sResult.GetSession();
                session.ExecuteDataQuery("INSERT INTO `/Root/.external_data/kv_settings` (key, value) VALUES ('/Root/olapStore.tier1.a', 'b')"
                    , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
                });

            const TInstant start = Now();
            while (!emulator->IsFound() && Now() - start < TDuration::Seconds(20)) {
                runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
            }
            Y_VERIFY(emulator->IsFound());
        }
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }

    /*
    Y_UNIT_TEST(DSConfigs) {
        TPortManager pm;

        ui32 grpcPort = pm.GetPort();
        ui32 msgbPort = pm.GetPort();

        Tests::TServerSettings serverSettings(msgbPort);
        serverSettings.Port = msgbPort;
        serverSettings.GrpcPort = grpcPort;
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableMetadataProvider(true)
            .SetEnableKqpSessionActor(true)
            .SetEnableOlapSchemaOperations(true);
        ;

        Tests::TServer::TPtr server = new Tests::TServer(serverSettings);
        server->EnableGRpc(grpcPort);
//        server->SetupDefaultProfiles();

        Tests::TClient client(serverSettings);

        auto& runtime = *server->GetRuntime();

        auto sender = runtime.AllocateEdgeActor();
        server->InitRoot(sender);
        TLocalHelper lHelper(*server);
        lHelper.CreateTestOlapTable();

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_TRACE);
        const TInstant start = Now();
        while (Now() - start < TDuration::Seconds(10)) {
            runtime.WaitForEdgeEvents([](TTestActorRuntimeBase& , TAutoPtr<IEventHandle>& event) {
                Cerr << "Step " << event->Type << Endl;
                return false;
                }, {}, TDuration::Seconds(1));
            Sleep(TDuration::Seconds(1));
            Cerr << "Step finished" << Endl;
        }

        NYdb::NTable::TTableClient tClient(server->GetDriver(), NYdb::NTable::TClientSettings().UseQueryCache(false));
        tClient.CreateSession().Subscribe([](NThreading::TFuture<NYdb::NTable::TCreateSessionResult> f) {
            auto sResult = f.GetValueSync();
            Cerr << sResult.GetIssues().ToString() << Endl;
            auto session = sResult.GetSession();
            session.ExecuteDataQuery("INSERT INTO `/Root/.external_data/kv_settings` (key, value) VALUES ('/Root/olapStore.tier1.a', 'b')"
                , NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx());
            });

        bool found = false;
        bool* foundPtr = &found;
        const auto pred = [foundPtr](TTestActorRuntimeBase& , TAutoPtr<IEventHandle>& event)->TTestActorRuntimeBase::EEventAction {
            if (event->HasBuffer() && !event->HasEvent()) {
            } else if (!event->GetBase()) {
                Cerr << "Type nullptr" << Endl;
            } else {
                Cerr << "Step " << event->GetBase()->Type() << Endl;
                auto ptr = dynamic_cast<NMetadataProvider::TEvRefreshSubscriberData*>(event->GetBase());
                if (ptr) {
                    auto value = ptr->GetSnapshotAs<TCSKVSnapshot>()->GetValue("/Root/olapStore.tier1.a");
                    if (value && *value == "b") {
                        *foundPtr = true;
                    } else {
                        Cerr << ptr->GetSnapshot()->SerializeToString() << Endl;
                    }
                }
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        };

        runtime.SetObserverFunc(pred);

        while (!found) {
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
        }
        //runtime.SetLogPriority(NKikimrServices::TX_PROXY, NLog::PRI_TRACE);
        //runtime.SetLogPriority(NKikimrServices::KQP_YQL, NLog::PRI_TRACE);
    }
*/
    Y_UNIT_TEST(CreateTable) {
        ui64 tableId = 1;

        TVector<TTypeId> intTypes = {
            NTypeIds::Timestamp,
            NTypeIds::Int8,
            NTypeIds::Int16,
            NTypeIds::Int32,
            NTypeIds::Int64,
            NTypeIds::Uint8,
            NTypeIds::Uint16,
            NTypeIds::Uint32,
            NTypeIds::Uint64,
            NTypeIds::Date,
            NTypeIds::Datetime
        };

        auto schema = TTestSchema::YdbSchema({"k0", TTypeInfo(NTypeIds::Timestamp)});
        auto pk = schema;
        pk.resize(4);

        for (auto& ydbType : intTypes) {
            schema[0].second = TTypeInfo(ydbType);
            pk[0].second = TTypeInfo(ydbType);
            auto txBody = TTestSchema::CreateTableTxBody(tableId, schema, pk);
            bool ok = TestCreateTable(txBody);
            UNIT_ASSERT(ok);
        }

        // TODO: support float types
        TVector<TTypeId> floatTypes = {
            NTypeIds::Float,
            NTypeIds::Double
        };

        for (auto& ydbType : floatTypes) {
            schema[0].second = TTypeInfo(ydbType);
            pk[0].second = TTypeInfo(ydbType);
            auto txBody = TTestSchema::CreateTableTxBody(tableId, schema, pk);
            bool ok = TestCreateTable(txBody);
            UNIT_ASSERT(!ok);
        }

        TVector<TTypeId> strTypes = {
            NTypeIds::String,
            NTypeIds::Utf8
        };

        for (auto& ydbType : strTypes) {
            schema[0].second = TTypeInfo(ydbType);
            pk[0].second = TTypeInfo(ydbType);
            auto txBody = TTestSchema::CreateTableTxBody(tableId, schema, pk);
            bool ok = TestCreateTable(txBody);
            UNIT_ASSERT(ok);
        }

        TVector<TTypeId> xsonTypes = {
            NTypeIds::Yson,
            NTypeIds::Json,
            NTypeIds::JsonDocument
        };

        for (auto& ydbType : xsonTypes) {
            schema[0].second = TTypeInfo(ydbType);
            pk[0].second = TTypeInfo(ydbType);
            auto txBody = TTestSchema::CreateTableTxBody(tableId, schema, pk);
            bool ok = TestCreateTable(txBody);
            UNIT_ASSERT(!ok);
        }
    }

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
        TestHotAndColdTiers(false);
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
}

}
