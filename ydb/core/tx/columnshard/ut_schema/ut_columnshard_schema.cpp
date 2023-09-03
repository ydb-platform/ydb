#include "columnshard_ut_common.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/wrappers/ut_helpers/s3_mock.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/services/metadata/service.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <library/cpp/actors/core/av_bootstrapped.h>

#include <util/system/hostname.h>

namespace NKikimr {

using namespace NTxUT;
using namespace NColumnShard;
using NWrappers::NTestHelpers::TS3Mock;

enum class EInitialEviction {
    None,
    Ttl,
    Tiering
};

class TDisableCompactionController: public NKikimr::NYDBTest::ICSController {
protected:
    virtual bool DoOnStartCompaction(std::shared_ptr<NOlap::TColumnEngineChanges>& changes) {
        changes = nullptr;
        return true;
    }
public:
};

namespace {

static const std::vector<std::pair<TString, TTypeInfo>> testYdbSchema = TTestSchema::YdbSchema();
static const std::vector<std::pair<TString, TTypeInfo>> testYdbPk = TTestSchema::YdbPkSchema();

std::shared_ptr<arrow::RecordBatch> UpdateColumn(std::shared_ptr<arrow::RecordBatch> batch, TString columnName, i64 seconds) {
    std::string name(columnName.c_str(), columnName.size());

    auto schema = batch->schema();
    int pos = schema->GetFieldIndex(name);
    UNIT_ASSERT(pos >= 0);
    auto colType = batch->GetColumnByName(name)->type_id();

    std::shared_ptr<arrow::Array> array;
    if (colType == arrow::Type::TIMESTAMP) {
        auto scalar = arrow::TimestampScalar(seconds * 1000 * 1000, arrow::timestamp(arrow::TimeUnit::MICRO));
        UNIT_ASSERT_VALUES_EQUAL(scalar.value, seconds * 1000 * 1000);

        auto res = arrow::MakeArrayFromScalar(scalar, batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT16) { // YQL Date
        TInstant date(TInstant::Seconds(seconds));
        auto res = arrow::MakeArrayFromScalar(arrow::UInt16Scalar(date.Days()), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT32) { // YQL Datetime or Uint32
        auto res = arrow::MakeArrayFromScalar(arrow::UInt32Scalar(seconds), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    } else if (colType == arrow::Type::UINT64) {
        auto res = arrow::MakeArrayFromScalar(arrow::UInt64Scalar(seconds), batch->num_rows());
        UNIT_ASSERT(res.ok());
        array = *res;
    }

    UNIT_ASSERT(array);

    auto columns = batch->columns();
    columns[pos] = array;
    return arrow::RecordBatch::Make(schema, batch->num_rows(), columns);
}

bool TriggerTTL(TTestBasicRuntime& runtime, TActorId& sender, NOlap::TSnapshot snap, const std::vector<ui64>& pathIds,
                ui64 tsSeconds, const TString& ttlColumnName) {
    TString txBody = TTestSchema::TtlTxBody(pathIds, ttlColumnName, tsSeconds);
    auto event = std::make_unique<TEvColumnShard::TEvProposeTransaction>(
        NKikimrTxColumnShard::TX_KIND_TTL, sender, snap.GetTxId(), txBody);

    ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, event.release());
    auto ev = runtime.GrabEdgeEvent<TEvColumnShard::TEvProposeTransactionResult>(sender);
    const auto& res = ev->Get()->Record;
    UNIT_ASSERT_EQUAL(res.GetTxId(), snap.GetTxId());
    UNIT_ASSERT_EQUAL(res.GetTxKind(), NKikimrTxColumnShard::TX_KIND_TTL);
    return (res.GetStatus() == NKikimrTxColumnShard::SUCCESS);
}

std::shared_ptr<arrow::Array> DeserializeColumn(const TString& blob, const TString& strSchema,
                                                const std::string& columnName)
{
    auto schema = NArrow::DeserializeSchema(strSchema);
    auto batch = NArrow::DeserializeBatch(blob, schema);
    UNIT_ASSERT(batch);

    //Cerr << "Got data batch (" << batch->num_rows() << " rows): " <<  batch->ToString() << "\n";

    std::shared_ptr<arrow::Array> array = batch->GetColumnByName(columnName);
    UNIT_ASSERT(array);
    return array;
}

bool CheckSame(const TString& blob, const TString& strSchema, ui32 expectedSize,
               const std::string& columnName, i64 seconds) {
    auto tsCol = DeserializeColumn(blob, strSchema, columnName);
    UNIT_ASSERT(tsCol);
    UNIT_ASSERT_VALUES_EQUAL(tsCol->length(), expectedSize);

    std::shared_ptr<arrow::Scalar> expected;
    switch (tsCol->type_id()) {
        case arrow::Type::TIMESTAMP:
            expected = std::make_shared<arrow::TimestampScalar>(seconds * 1000 * 1000,
                                                                arrow::timestamp(arrow::TimeUnit::MICRO));
            break;
        case arrow::Type::UINT16:
            expected = std::make_shared<arrow::UInt16Scalar>(TInstant::Seconds(seconds).Days());
            break;
        case arrow::Type::UINT32:
            expected = std::make_shared<arrow::UInt32Scalar>(seconds);
            break;
        case arrow::Type::UINT64:
            expected = std::make_shared<arrow::UInt64Scalar>(seconds);
            break;
        default:
            break;
    }

    UNIT_ASSERT(expected);

    for (int i = 0; i < tsCol->length(); ++i) {
        auto value = *tsCol->GetScalar(i);
        if (!value->Equals(*expected)) {
            Cerr << "Unexpected: '" << value->ToString() << "', expected " << expected->ToString() << "\n";
            return false;
        }
    }
    return true;
}

std::vector<TString> MakeData(const std::vector<ui64>& ts, ui32 portionSize, ui32 overlapSize, const TString& ttlColumnName,
                              const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema = testYdbSchema) {
    UNIT_ASSERT(ts.size() > 0);

    ui32 numRows = portionSize + (ts.size() - 1) * (portionSize - overlapSize);
    TString testData = MakeTestBlob({0, numRows}, ydbSchema);
    auto schema = NArrow::MakeArrowSchema(ydbSchema);
    auto testBatch = NArrow::DeserializeBatch(testData, schema);

    std::vector<TString> data;
    data.reserve(ts.size());
    for (size_t i = 0; i < ts.size(); ++i) {
        auto batch = testBatch->Slice((portionSize - overlapSize) * i, portionSize);
        batch = UpdateColumn(batch, ttlColumnName, ts[i]);
        data.emplace_back(NArrow::SerializeBatchNoCompression(batch));
        UNIT_ASSERT(data.back().size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT);
    }

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
    return ProposeSchemaTx(runtime, sender, txBody, NOlap::TSnapshot(++planStep, ++txId));
}

enum class EExpectedResult {
    OK_FINISHED,
    OK,
    ERROR
};

TString GetReadResult(NKikimrTxColumnShard::TEvReadResult& resRead, EExpectedResult expected = EExpectedResult::OK_FINISHED)
{
    Cerr << "Got batchNo: " << resRead.GetBatch() << "\n";

    UNIT_ASSERT_EQUAL(resRead.GetOrigin(), TTestTxConfig::TxTablet0);
    UNIT_ASSERT_EQUAL(resRead.GetTxInitiator(), TTestTxConfig::TxTablet1);
    if (expected == EExpectedResult::ERROR) {
        UNIT_ASSERT_EQUAL(resRead.GetStatus(), NKikimrTxColumnShard::EResultStatus::ERROR);
    } else {
        UNIT_ASSERT_EQUAL(resRead.GetStatus(), NKikimrTxColumnShard::EResultStatus::SUCCESS);
    }
    if (expected == EExpectedResult::OK_FINISHED) {
        UNIT_ASSERT_EQUAL(resRead.GetFinished(), true);
    }
    return resRead.GetData();
}

static constexpr ui32 PORTION_ROWS = 80 * 1000;

// ts[0] = 1600000000; // date -u --date='@1600000000' Sun Sep 13 12:26:40 UTC 2020
// ts[1] = 1620000000; // date -u --date='@1620000000' Mon May  3 00:00:00 UTC 2021
void TestTtl(bool reboots, bool internal, TTestSchema::TTableSpecials spec = {},
             const std::vector<std::pair<TString, TTypeInfo>>& ydbSchema = testYdbSchema)
{
    auto csControllerGuard = NKikimr::NYDBTest::TControllers::RegisterCSControllerGuard<TDisableCompactionController>();
    std::vector<ui64> ts = {1600000000, 1620000000};

    ui32 ttlIncSeconds = 1;
    for (auto& [name, typeInfo] : ydbSchema) {
        if (name == spec.TtlColumn) {
            if (typeInfo.GetTypeId() == NTypeIds::Date) {
                ttlIncSeconds = TDuration::Days(1).Seconds();
            }
            break;
        }
    }

    TTestBasicRuntime runtime;
    TTester::Setup(runtime);
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

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

    ui32 ttlSec = TAppData::TimeProvider->Now().Seconds(); // disable internal tll
    if (internal) {
        ttlSec -= (ts[0] + ts[1]) / 2; // enable internal ttl between ts1 and ts2
    }
    if (spec.HasTiers()) {
        spec.Tiers[0].EvictAfter = TDuration::Seconds(ttlSec);
    } else {
        UNIT_ASSERT(!spec.TtlColumn.empty());
        spec.EvictAfter = TDuration::Seconds(ttlSec);
    }
    bool ok = ProposeSchemaTx(runtime, sender,
                              TTestSchema::CreateInitShardTxBody(tableId, ydbSchema, testYdbPk, spec, "/Root/olapStore"),
                              NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
    if (spec.HasTiers()) {
        ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(spec));
    }
    //

    auto blobs = MakeData(ts, PORTION_ROWS, PORTION_ROWS / 2, spec.TtlColumn, ydbSchema);
    UNIT_ASSERT_EQUAL(blobs.size(), 2);
    for (auto& data : blobs) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, ++writeId, tableId, data, ydbSchema, true, &writeIds));
        ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, ++planStep, txId);
    }

    // TODO: write into path 2 (no ttl)

    if (reboots) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }

    if (internal) {
        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {}, 0, spec.TtlColumn);
    } else {
        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {tableId}, ts[0] + ttlIncSeconds, spec.TtlColumn);
    }

    TAutoPtr<IEventHandle> handle;

    if (reboots) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }

    {
        --planStep;
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId);
        Proto(read.get()).AddColumnNames(spec.TtlColumn);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle);
        UNIT_ASSERT(event);

        auto& resRead = Proto(event);
        TString data = GetReadResult(resRead);
        UNIT_ASSERT_VALUES_EQUAL(resRead.GetBatch(), 0);
        UNIT_ASSERT(data.size() > 0);

        auto& schema = resRead.GetMeta().GetSchema();
        UNIT_ASSERT(CheckSame(data, schema, PORTION_ROWS, spec.TtlColumn, ts[1]));
    }

    // Alter TTL
    ttlSec = TAppData::TimeProvider->Now().Seconds() - (ts[1] + 1);
    if (spec.HasTiers()) {
        spec.Tiers[0].EvictAfter = TDuration::Seconds(ttlSec);
    } else {
        spec.EvictAfter = TDuration::Seconds(ttlSec);
    }
    ok = ProposeSchemaTx(runtime, sender,
                         TTestSchema::AlterTableTxBody(tableId, 2, spec),
                         NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
    if (spec.HasTiers()) {
        ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(spec));
    }

    if (internal) {
        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {}, 0, spec.TtlColumn);
    } else {
        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {tableId}, ts[1] + ttlIncSeconds, spec.TtlColumn);
    }

    {
        --planStep;
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId);
        Proto(read.get()).AddColumnNames(spec.TtlColumn);

        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
        auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle);
        UNIT_ASSERT(event);

        auto& resRead = Proto(event);
        TString data = GetReadResult(resRead);
        UNIT_ASSERT_VALUES_EQUAL(resRead.GetBatch(), 0);
        UNIT_ASSERT_VALUES_EQUAL(data.size(), 0);
    }

    // Disable TTL
    ok = ProposeSchemaTx(runtime, sender,
                         TTestSchema::AlterTableTxBody(tableId, 3, TTestSchema::TTableSpecials()),
                         NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    if (spec.HasTiers()) {
        ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(TTestSchema::TTableSpecials()));
    }
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));


    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(runtime, sender, ++writeId, tableId, blobs[0], ydbSchema, true, &writeIds));
    ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, ++planStep, txId);

    if (internal) {
        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {}, 0, spec.TtlColumn);
    } else {
        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {tableId}, ts[0] - ttlIncSeconds, spec.TtlColumn);
    }

    {
        --planStep;
        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep, Max<ui64>(), tableId);
        Proto(read.get()).AddColumnNames(spec.TtlColumn);

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
        UNIT_ASSERT(CheckSame(resRead.GetData(), schema, PORTION_ROWS, spec.TtlColumn, ts[0]));
    }
}

class TCountersContainer {
private:
    struct TCounters {
        ui32 Attempt = 0;
        ui32 Request = 0;
        ui32 Response = 0;
        ui32 Success = 0;

        void Clear() {
            Attempt = 0;
            Request = 0;
            Response = 0;
            Success = 0;
        }

        TString ToString() const {
            return TStringBuilder() << Attempt << "/" << Request << "/" << Response << "/" << Success;
        }
    };

    ui32 WaitNo = 0;

public:
    TCounters ExportCounters;
    TCounters ForgetCounters;
    ui32 CaptureReadEvents = 0;
    std::vector<TAutoPtr<IEventHandle>> CapturedReads;
    ui32 CaptureEvictResponse = 0;
    ui32 CaptureForgetResponse = 0;
    std::vector<TAutoPtr<IEventHandle>> CapturedResponses;
    bool BlockForgets = false;

    void WaitEvents(TTestBasicRuntime& runtime, const TDuration& timeout, ui32 waitExports, ui32 waitForgets,
                    const TString& promo = "START_WAITING") {
        const TInstant startInstant = TAppData::TimeProvider->Now();
        const TInstant deadline = startInstant + timeout;
        Cerr << promo << "(" << WaitNo << "): "
            << "E" << ExportCounters.ToString() << " F" << ForgetCounters.ToString() << Endl;
        while (TAppData::TimeProvider->Now() < deadline) {
            Cerr << "IN_WAITING(" << WaitNo << "): "
                << "E" << ExportCounters.ToString() << " F" << ForgetCounters.ToString() << Endl;
            runtime.SimulateSleep(TDuration::Seconds(1));

            if (!waitExports && ExportCounters.Success
                || !waitForgets && ForgetCounters.Success
                || !waitForgets && ExportCounters.Success >= waitExports
                || !waitExports && ForgetCounters.Success >= waitForgets
                || waitExports && waitForgets
                    && ExportCounters.Success >= waitExports && ForgetCounters.Success >= waitForgets) {
                break;
            }
        }
        Cerr << "FINISH_WAITING(" << WaitNo << "): "
            << "E" << ExportCounters.ToString() << " F" << ForgetCounters.ToString() << Endl;
        UNIT_ASSERT_VALUES_EQUAL(ExportCounters.Success, waitExports);
        UNIT_ASSERT_VALUES_EQUAL(ForgetCounters.Success, waitForgets);
        ExportCounters.Clear();
        ForgetCounters.Clear();
        ++WaitNo;
    }

    void WaitMoreEvents(TTestBasicRuntime& runtime, const TDuration& timeout, ui32 waitExports, ui32 waitForgets) {
        --WaitNo;
        WaitEvents(runtime, timeout, waitExports, waitForgets, "CONTINUE_WAITING");
    }

    void WaitReadsCaptured(TTestBasicRuntime& runtime) const {
        const TInstant startInstant = TAppData::TimeProvider->Now();
        const TInstant deadline = startInstant + TDuration::Seconds(10);
        while (CaptureReadEvents && TAppData::TimeProvider->Now() < deadline) {
            runtime.SimulateSleep(TDuration::Seconds(1));
        }
        UNIT_ASSERT_VALUES_EQUAL(CaptureReadEvents, 0);
    }

    void ResendCapturedReads(TTestBasicRuntime& runtime) {
        for (auto& cev : CapturedReads) {
            auto* msg = TryGetPrivateEvent<NBlobCache::TEvBlobCache::TEvReadBlobRange>(cev);
            UNIT_ASSERT(msg);
            Cerr << "RESEND " << msg->BlobRange.ToString() << " "
                    << msg->ReadOptions.ToString() << Endl;
            runtime.Send(cev.Release());
        }
        CapturedReads.clear();
    }

    void ResendCapturedResponses(TTestBasicRuntime& runtime) {
        for (auto& cev : CapturedResponses) {
            Cerr << "RESEND S3_RESPONSE" << Endl;
            runtime.Send(cev.Release());
        }
        CapturedResponses.clear();
    }

    void BlockForgetsTillReboot() {
        BlockForgets = true;
    }
};

class TEventsCounter {
private:
    TCountersContainer* Counters = nullptr;
    TTestBasicRuntime& Runtime;

public:
    TEventsCounter(TCountersContainer& counters, TTestBasicRuntime& runtime)
        : Counters(&counters)
        , Runtime(runtime)
    {
        Y_UNUSED(Runtime);
    }

    bool operator()(TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& ev) {
        TStringBuilder ss;
        if (ev->GetTypeRewrite() == TEvTablet::EvBoot) {
            Counters->BlockForgets = false;
            return false;
        } else if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvExport>(ev)) {
            if (msg->Status == NKikimrProto::OK) {
                ss << "EXPORT(done " << ++Counters->ExportCounters.Success << "): ";
            } else {
                ss << "EXPORT(attempt " << ++Counters->ExportCounters.Attempt << "): "
                    << NKikimrProto::EReplyStatus_Name(msg->Status);
            }
        } else if (auto* msg = TryGetPrivateEvent<NColumnShard::TEvPrivate::TEvForget>(ev)) {
            if (Counters->BlockForgets) {
                ss << "FORGET(ignore " << NKikimrProto::EReplyStatus_Name(msg->Status) << "): ";
                ss << " " << ev->Sender << "->" << ev->Recipient;
                Cerr << ss << Endl;
                return true;
            }

            if (msg->Status == NKikimrProto::OK) {
                ss << "FORGET(done " << ++Counters->ForgetCounters.Success << "): ";
            } else {
                ss << "FORGET(attempt " << ++Counters->ForgetCounters.Attempt << "): "
                    << NKikimrProto::EReplyStatus_Name(msg->Status);
            }
        } else if (auto* msg = TryGetPrivateEvent<NWrappers::NExternalStorage::TEvPutObjectRequest>(ev)) {
            ss << "S3_REQ(put " << ++Counters->ExportCounters.Request << "):";
        } else if (auto* msg = TryGetPrivateEvent<NWrappers::NExternalStorage::TEvPutObjectResponse>(ev)) {
            if (Counters->CaptureEvictResponse) {
                Cerr << "CAPTURE S3_RESPONSE(put)" << Endl;
                --Counters->CaptureEvictResponse;
                Counters->CapturedResponses.push_back(ev.Release());
                return true;
            }

            ss << "S3_RESPONSE(put " << ++Counters->ExportCounters.Response << "):";
        } else if (auto* msg = TryGetPrivateEvent<NWrappers::NExternalStorage::TEvDeleteObjectRequest>(ev)) {
            ss << "S3_REQ(delete " << ++Counters->ForgetCounters.Request << "):";
        } else if (auto* msg = TryGetPrivateEvent<NWrappers::NExternalStorage::TEvDeleteObjectResponse>(ev)) {
            if (Counters->CaptureForgetResponse) {
                Cerr << "CAPTURE S3_RESPONSE(delete)" << Endl;
                --Counters->CaptureForgetResponse;
                Counters->CapturedResponses.push_back(ev.Release());
                return true;
            }

            ss << "S3_RESPONSE(delete " << ++Counters->ForgetCounters.Response << "):";
        } else if (auto* msg = TryGetPrivateEvent<NBlobCache::TEvBlobCache::TEvReadBlobRange>(ev)) {
            if (Counters->CaptureReadEvents) {
                Cerr << "CAPTURE " << msg->BlobRange.ToString() << " "
                    << msg->ReadOptions.ToString() << Endl;
                --Counters->CaptureReadEvents;
                Counters->CapturedReads.push_back(ev.Release());
                return true;
            } else {
                return false;
            }
        } else if (auto* msg = TryGetPrivateEvent<TEvColumnShard::TEvReadResult>(ev)) {
            ss << "Got TEvReadResult " << NKikimrTxColumnShard::EResultStatus_Name(Proto(msg).GetStatus()) << Endl;
        } else {
            return false;
        }
        ss << " " << ev->Sender << "->" << ev->Recipient;
        Cerr << ss << Endl;
        return false;
    }
};

std::vector<std::pair<ui32, ui64>> TestTiers(bool reboots, const std::vector<TString>& blobs,
                                             const std::vector<TTestSchema::TTableSpecials>& specs,
                                             const THashSet<ui32>& exportSteps,
                                             const THashSet<ui32>& forgetSteps,
                                             std::optional<ui32> eventLoss = {})
{
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD_SCAN, NActors::NLog::PRI_INFO);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime,
                           CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard),
                           &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    // Disable blob cache. It hides evict-delete, evict-read races.
    {
        TAtomic unused;
        runtime.GetAppData().Icb->SetValue("BlobCache.MaxCacheDataSize", 0, unused);
    }

    // Disable GC batching so that deleted blobs get collected without a delay
    {
        TAtomic unused;
        runtime.GetAppData().Icb->SetValue("ColumnShardControls.BlobCountToTriggerGC", 1, unused);
    }

    //

    ui64 metaShard = TTestTxConfig::TxTablet1;
    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 planStep = 1000000000; // greater then delays
    ui64 txId = 100;
    const TDuration exportTimeout = TDuration::Seconds(40);

    UNIT_ASSERT(specs.size() > 0);
    {
        const bool ok = ProposeSchemaTx(runtime, sender,
            TTestSchema::CreateInitShardTxBody(tableId, testYdbSchema, testYdbPk, specs[0], "/Root/olapStore"),
            NOlap::TSnapshot(++planStep, ++txId));
        UNIT_ASSERT(ok);
    }
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
    if (specs[0].Tiers.size()) {
        ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(specs[0]));
    }

    for (auto& data : blobs) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, ++writeId, tableId, data, testYdbSchema, true, &writeIds));
        ProposeCommit(runtime, sender, ++txId, writeIds);
        PlanCommit(runtime, sender, ++planStep, txId);
    }

    if (reboots) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }

    runtime.SetLogPriority(NKikimrServices::TX_COLUMNSHARD, NActors::NLog::PRI_DEBUG);

    TAutoPtr<IEventHandle> handle;

    std::vector<std::pair<ui32, ui64>> specRowsBytes;
    specRowsBytes.reserve(specs.size());
    ui32 deplayedExports = 0;
    ui32 deplayedForgets = 0;

    TCountersContainer counter;
    runtime.SetEventFilter(TEventsCounter(counter, runtime));
    for (ui32 i = 0; i < specs.size(); ++i) {
        ui32 numExports = exportSteps.contains(i) ? 1 : 0;
        ui32 numForgets = forgetSteps.contains(i) ? 1 : 0;
        bool hasColdEviction = false;
        bool misconfig = false;
        auto expectedReadResult = EExpectedResult::OK;
        for (auto&& spec : specs[i].Tiers) {
            if (!!spec.S3) {
                hasColdEviction = true;
                if (spec.S3->GetEndpoint() != "fake") {
                    misconfig = true;
                    // misconfig in export => OK, misconfig after export => ERROR
                    if (i > 1) {
                        expectedReadResult = EExpectedResult::ERROR;
                    }
                    deplayedExports += numExports;
                    deplayedForgets += numForgets;
                    numExports = 0;
                    numForgets = 0;
                }
                break;
            }
        }
        if (i) {
            const ui32 version = i + 1;
            const bool ok = ProposeSchemaTx(runtime, sender,
                TTestSchema::AlterTableTxBody(tableId, version, specs[i]),
                NOlap::TSnapshot(++planStep, ++txId));
            UNIT_ASSERT(ok);
            PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
        }
        if (specs[i].HasTiers() || reboots) {
            ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(specs[i]));
        }

        if (!misconfig && (deplayedExports || deplayedForgets)) {
            UNIT_ASSERT(hasColdEviction);
            // continue waiting: finish previous step
            counter.WaitMoreEvents(runtime, exportTimeout, deplayedExports, deplayedForgets);
            deplayedExports = 0;
            deplayedForgets = 0;
        }

        if (eventLoss) {
            if (*eventLoss == i) {
                if (numExports) {
                    counter.CaptureEvictResponse = 1;
                    deplayedExports += numExports;
                    numExports = 0;
                } else if (numForgets) {
                    counter.CaptureForgetResponse = 1;
                    deplayedForgets += numForgets;
                    numForgets = 0;
                }
            } else {
                // Check there would be no troubles with delayed responses
                counter.ResendCapturedResponses(runtime);
            }
        }

        // Read crossed with eviction (start)
        if (!misconfig) {
            auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep - 1, Max<ui64>(), tableId);
            Proto(read.get()).AddColumnNames(specs[i].TtlColumn);

            counter.CaptureReadEvents = specs[i].WaitEmptyAfter ? 0 : 1; // TODO: we need affected by tiering blob here
            ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());
            counter.WaitReadsCaptured(runtime);
        }

        // Eviction

        TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {}, 0, specs[i].TtlColumn);

        Cerr << "-- " << (hasColdEviction ? "COLD" : "HOT")
            << " TIERING(" << i << ") num tiers: " << specs[i].Tiers.size()
            << ", exports: " << numExports << ", forgets: " << numForgets
            << ", delayed exports: " << deplayedExports << ", delayed forgets: " << deplayedForgets << Endl;

        if (numExports) {
            UNIT_ASSERT(hasColdEviction);
            counter.WaitEvents(runtime, exportTimeout, numExports, 0);
        } else {
            TDuration timeout = hasColdEviction ? TDuration::Seconds(10) : TDuration::Seconds(4);
            counter.WaitEvents(runtime, timeout, 0, 0);
        }

        if (numForgets && reboots) {
            // Do not finish forget before reboot. Check forget would happen after it.
            counter.BlockForgetsTillReboot();
        }

        // Read crossed with eviction (finish)
        if (!misconfig) {
            counter.ResendCapturedReads(runtime);
            ui32 numBatches = 0;
            THashSet<ui32> batchNumbers;
            while (!numBatches || numBatches < batchNumbers.size()) {
                auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle);
                UNIT_ASSERT(event);

                auto& resRead = Proto(event);
                TString data = GetReadResult(resRead, EExpectedResult::OK);

                batchNumbers.insert(resRead.GetBatch());
                if (resRead.GetFinished()) {
                    numBatches = resRead.GetBatch() + 1;
                }
            }
        }

        if (numForgets) {
            UNIT_ASSERT(hasColdEviction);
            if (reboots) {
                Cerr << "INTERMEDIATE REBOOT(" << i << ")" << Endl;
                RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
                ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(specs[i]));
            }
            counter.WaitMoreEvents(runtime, exportTimeout, 0, numForgets);
        }

        // Read data after eviction
        TString columnToRead = specs[i].TtlColumn;

        auto read = std::make_unique<TEvColumnShard::TEvRead>(sender, metaShard, planStep-1, Max<ui64>(), tableId);
        Proto(read.get()).AddColumnNames(columnToRead);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, read.release());

        specRowsBytes.emplace_back(0, 0);
        ui32 numBatches = 0;
        ui32 numExpected = (expectedReadResult == EExpectedResult::ERROR) ? 1 : 100;
        for (; numBatches < numExpected; ++numBatches) {
            auto event = runtime.GrabEdgeEvent<TEvColumnShard::TEvReadResult>(handle);
            UNIT_ASSERT(event);

            auto& resRead = Proto(event);
            TString data = GetReadResult(resRead, expectedReadResult);
            if (expectedReadResult == EExpectedResult::ERROR) {
                break;
            }
            if (!data.size()) {
                UNIT_ASSERT(resRead.GetFinished());
                break;
            }

            auto& meta = resRead.GetMeta();
            auto& schema = meta.GetSchema();
            auto ttlColumn = DeserializeColumn(resRead.GetData(), schema, columnToRead);
            UNIT_ASSERT(ttlColumn);

            specRowsBytes.back().first += ttlColumn->length();
            if (resRead.GetFinished()) {
                UNIT_ASSERT(meta.HasReadStats());
                auto& readStats = meta.GetReadStats();
                ui64 numBytes = readStats.GetPortionsBytes(); // compressed bytes in storage
                specRowsBytes.back().second += numBytes;
                numExpected = resRead.GetBatch() + 1;
            }
        }
        UNIT_ASSERT(numBatches < 100);

        if (reboots) {
            Cerr << "REBOOT(" << i << ")" << Endl;
            RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
        }
    }

    return specRowsBytes;
}

class TEvictionChanges {
public:
    static std::vector<TTestSchema::TTableSpecials> OneTierAlters(const TTestSchema::TTableSpecials& spec,
                                                                const std::vector<ui64>& ts) {
        TInstant now = TAppData::TimeProvider->Now();
        TDuration allowBoth = TDuration::Seconds(now.Seconds() - ts[0] + 600);
        TDuration allowOne = TDuration::Seconds(now.Seconds() - ts[1] + 600);
        TDuration allowNone = TDuration::Seconds(now.Seconds() - ts[1] - 600);

        std::vector<TTestSchema::TTableSpecials> alters = { TTestSchema::TTableSpecials() };
        AddTierAlters(spec, {allowBoth, allowOne, allowNone}, alters);
        return alters;
    }

    static void AddTierAlters(const TTestSchema::TTableSpecials& spec, const std::vector<TDuration>&& borders,
                        std::vector<TTestSchema::TTableSpecials>& alters) {
        UNIT_ASSERT_EQUAL(borders.size(), 3);
        UNIT_ASSERT(spec.Tiers.size());

        alters.reserve(alters.size() + spec.Tiers.size() + 1);

        if (spec.Tiers.size() == 1) {
            alters.push_back(MakeAlter(spec, {borders[0]})); // <tier0 border>, data[0], data[1]
            alters.push_back(MakeAlter(spec, {borders[1]})); // data[0], <tier0 border>, data[1]
            alters.push_back(MakeAlter(spec, {borders[2]})); // data[0], data[1], <tier0 border>
        } else if (spec.Tiers.size() == 2) {
            alters.push_back(MakeAlter(spec, {borders[0], borders[0]})); // <tier1 border>, <tier0 border>, data[0], data[1]
            alters.push_back(MakeAlter(spec, {borders[1], borders[0]})); // <tier1 border>, data[0], <tier0 border>, data[1]
            alters.push_back(MakeAlter(spec, {borders[2], borders[1]})); // data[0], <tier1 border>, data[1], <tier0 border>
            alters.push_back(MakeAlter(spec, {borders[2], borders[2]})); // data[0], data[1], <tier1 border>, <tier0 border>
        }
    }

    static void AddTtlAlters(const TTestSchema::TTableSpecials& spec, const std::vector<TDuration>&& borders,
                      std::vector<TTestSchema::TTableSpecials>& alters) {
        UNIT_ASSERT_EQUAL(borders.size(), 3);
        UNIT_ASSERT(spec.Tiers.size());

        TTestSchema::TTableSpecials specTtl(spec);
        if (spec.Tiers.size() == 1) {
            specTtl = MakeAlter(spec, {borders[0]}); // <tier0 border>, data[0], data[1]
        } else if (spec.Tiers.size() == 2) {
            specTtl = MakeAlter(spec, {borders[0], borders[0]}); // <tier1 border>, <tier0 border>, data[0], data[1]
        }

        alters.reserve(alters.size() + borders.size());
        alters.push_back(specTtl.SetTtl(borders[0])); // <ttl border>, data[0], data[1]
        alters.push_back(specTtl.SetTtl(borders[1])); // data[0], <ttl border>, data[1]
        alters.push_back(specTtl.SetTtl(borders[2])); // data[0], data[1], <ttl border>
    }

    static void Assert(const TTestSchema::TTableSpecials& spec,
                       const std::vector<std::pair<ui32, ui64>>& rowsBytes,
                       size_t initialEviction) {
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[initialEviction].first, 2 * PORTION_ROWS);
        UNIT_ASSERT(rowsBytes[initialEviction].second);
        if (spec.Tiers.size() > 1) {
            UNIT_ASSERT_VALUES_EQUAL(rowsBytes[initialEviction].first, rowsBytes[initialEviction + 1].first);
        }

        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[rowsBytes.size() - 2].first, PORTION_ROWS);
        UNIT_ASSERT(rowsBytes[rowsBytes.size() - 2].second);

        UNIT_ASSERT_VALUES_EQUAL(rowsBytes.back().first, 0);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes.back().second, 0);
    }

private:
    static TTestSchema::TTableSpecials MakeAlter(const TTestSchema::TTableSpecials& spec,
                                          const std::vector<TDuration>& tierBorders) {
        UNIT_ASSERT_EQUAL(spec.Tiers.size(), tierBorders.size());

        TTestSchema::TTableSpecials alter(spec); // same TTL, Codec, etc.
        for (size_t i = 0; i < tierBorders.size(); ++i) {
            alter.Tiers[i].EvictAfter = tierBorders[i];
        }
        return alter;
    }
};

TTestSchema::TTableSpecials InitialSpec(const EInitialEviction init, TDuration initTs) {
    TTestSchema::TTableSpecials spec;
    if (init == EInitialEviction::Ttl) {
        spec.TtlColumn = "timestamp";
        spec.EvictAfter = initTs;
    }
    return spec;
}

std::vector<std::pair<ui32, ui64>> TestTiersAndTtl(const TTestSchema::TTableSpecials& spec, bool reboots,
                                                   EInitialEviction init, bool testTtl = false) {
    const std::vector<ui64> ts = { 1600000000, 1620000000 };

    ui32 overlapSize = 0; // TODO: 40 * 1000 (it should lead to fewer row count in result)
    std::vector<TString> blobs = MakeData(ts, PORTION_ROWS, overlapSize, spec.TtlColumn);
    if (init != EInitialEviction::Tiering) {
        std::vector<TString> preload = MakeData({ 1500000000, 1620000000 }, PORTION_ROWS, overlapSize, spec.TtlColumn);
        blobs.emplace_back(std::move(preload[0]));
        blobs.emplace_back(std::move(preload[1]));
    }

    TInstant now = TAppData::TimeProvider->Now();
    TDuration allowBoth = TDuration::Seconds(now.Seconds() - ts[0] + 600);
    TDuration allowOne = TDuration::Seconds(now.Seconds() - ts[1] + 600);
    TDuration allowNone = TDuration::Seconds(now.Seconds() - ts[1] - 600);

    std::vector<TTestSchema::TTableSpecials> alters = { InitialSpec(init, allowBoth) };
    size_t initialEviction = alters.size();

    TEvictionChanges changes;
    THashSet<ui32> exports;
    THashSet<ui32> forgets;
    if (testTtl) {
        changes.AddTtlAlters(spec, {allowBoth, allowOne, allowNone}, alters);
        alters.back().WaitEmptyAfter = true;
    } else {
        changes.AddTierAlters(spec, {allowBoth, allowOne, allowNone}, alters);

        for (ui32 i = initialEviction + 1; i < alters.size() - 1; ++i) {
            for (auto& tier : alters[i].Tiers) {
                if (tier.S3) {
                    exports.emplace(i);
                    break;
                }
            }
        }
        for (ui32 i = initialEviction + 2; i < alters.size(); ++i) {
            for (auto& tier : alters[i].Tiers) {
                if (tier.S3) {
                    forgets.emplace(i);
                    break;
                }
            }
        }
    }

    auto rowsBytes = TestTiers(reboots, blobs, alters, exports, forgets);
    for (auto&& i : rowsBytes) {
        Cerr << i.first << "/" << i.second << Endl;
    }

    UNIT_ASSERT_EQUAL(rowsBytes.size(), alters.size());

    if (!testTtl) { // TODO
        changes.Assert(spec, rowsBytes, initialEviction);
    }
    return rowsBytes;
}

std::vector<std::pair<ui32, ui64>> TestOneTierExport(const TTestSchema::TTableSpecials& spec,
                                                    const std::vector<TTestSchema::TTableSpecials>& alters,
                                                    const std::vector<ui64>& ts, bool reboots, std::optional<ui32> loss) {
    ui32 overlapSize = 0;
    std::vector<TString> blobs = MakeData(ts, PORTION_ROWS, overlapSize, spec.TtlColumn);

    auto rowsBytes = TestTiers(reboots, blobs, alters, {1}, {2, 3}, loss);
    for (auto&& i : rowsBytes) {
        Cerr << i.first << "/" << i.second << Endl;
    }

    UNIT_ASSERT_EQUAL(rowsBytes.size(), alters.size());
    return rowsBytes;
}

void TestTwoHotTiers(bool reboot, bool changeTtl, const EInitialEviction initial = EInitialEviction::None,
                    bool revCompaction = false) {
    TTestSchema::TTableSpecials spec;
    spec.SetTtlColumn("timestamp");
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier0").SetTtlColumn("timestamp"));
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier1").SetTtlColumn("timestamp"));
    spec.Tiers[(revCompaction ? 0 : 1)].SetCodec("zstd");

    auto rowsBytes = TestTiersAndTtl(spec, reboot, initial, changeTtl);
    if (changeTtl) {
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[0].first, 3 * PORTION_ROWS);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[1].first, 2 * PORTION_ROWS);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[2].first, PORTION_ROWS);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[3].first, 0);
    } else {
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes.size(), 5);
        if (initial == EInitialEviction::Ttl) {
            UNIT_ASSERT_VALUES_EQUAL(rowsBytes[0].first, 2 * PORTION_ROWS);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(rowsBytes[0].first, 3 * PORTION_ROWS);
        }
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[1].first, 2 * PORTION_ROWS);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[2].first, 2 * PORTION_ROWS);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[3].first, PORTION_ROWS);
        UNIT_ASSERT_VALUES_EQUAL(rowsBytes[4].first, 0);

        // compression works
        if (revCompaction) {
            UNIT_ASSERT(rowsBytes[1].second < rowsBytes[2].second);
        } else {
            UNIT_ASSERT(rowsBytes[1].second > rowsBytes[2].second);
        }
    }
}

void TestHotAndColdTiers(bool reboot, const EInitialEviction initial) {
    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock({}, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    TTestSchema::TTableSpecials spec;
    spec.SetTtlColumn("timestamp");
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier0").SetTtlColumn("timestamp"));
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("tier1").SetTtlColumn("timestamp"));
    spec.Tiers.back().S3 = TTestSchema::TStorageTier::FakeS3();

    TestTiersAndTtl(spec, reboot, initial);
}

struct TExportTestOpts {
    std::optional<ui32> Misconfig;
    std::optional<ui32> Loss;
    std::optional<ui32> NoTier;
};

void TestExport(bool reboot, TExportTestOpts&& opts = TExportTestOpts{}) {
    TPortManager portManager;
    const ui16 port = portManager.GetPort();

    TS3Mock s3Mock({}, TS3Mock::TSettings(port));
    UNIT_ASSERT(s3Mock.Start());

    TTestSchema::TTableSpecials spec;
    spec.SetTtlColumn("timestamp");
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("cold").SetTtlColumn("timestamp"));
    spec.Tiers.back().S3 = TTestSchema::TStorageTier::FakeS3();

    const std::vector<ui64> ts = { 1600000000, 1620000000 };
    TEvictionChanges changes;
    std::vector<TTestSchema::TTableSpecials> alters = changes.OneTierAlters(spec, ts);
    UNIT_ASSERT_VALUES_EQUAL(alters.size(), 4);

    if (opts.Misconfig) {
        ui32 alterNo = *opts.Misconfig;
        // Add error in config => eviction + not finished export
        UNIT_ASSERT_VALUES_EQUAL(alters[alterNo].Tiers.size(), 1);
        UNIT_ASSERT(alters[alterNo].Tiers[0].S3);
        alters[alterNo].Tiers[0].S3->SetEndpoint("nowhere"); // clear special "fake" endpoint
    }
    if (opts.NoTier) {
        ui32 alterNo = *opts.NoTier;
        // Add error in config => eviction + not finished export
        UNIT_ASSERT_VALUES_EQUAL(alters[alterNo].Tiers.size(), 1);
        UNIT_ASSERT(alters[alterNo].Tiers[0].S3);
        alters[alterNo].Tiers[0].S3 = {};
    }

    auto rowsBytes = TestOneTierExport(spec, alters, ts, reboot, opts.Loss);
    if (!opts.Misconfig) {
        changes.Assert(spec, rowsBytes, 1);
    }
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
                              NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));

    //

    TString data1 = MakeTestBlob({0, PORTION_ROWS}, testYdbSchema);
    UNIT_ASSERT(data1.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT);
    UNIT_ASSERT(data1.size() < 7 * 1024 * 1024);

    TString data2 = MakeTestBlob({0, 100}, testYdbSchema);
    UNIT_ASSERT(data2.size() < NColumnShard::TLimits::MIN_BYTES_TO_INSERT);

    // Write into index
    std::vector<ui64> writeIds;
    UNIT_ASSERT(WriteData(runtime, sender, ++writeId, tableId, data1, testYdbSchema, true, &writeIds));
    ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, ++planStep, txId);

    // Write into InsertTable
    writeIds.clear();
    UNIT_ASSERT(WriteData(runtime, sender, ++writeId, tableId, data2, testYdbSchema, true, &writeIds));
    ProposeCommit(runtime, sender, ++txId, writeIds);
    PlanCommit(runtime, sender, ++planStep, txId);

    if (reboots) {
        RebootTablet(runtime, TTestTxConfig::TxTablet0, sender);
    }

    // Drop table
    ok = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(tableId, 2), NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));

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
        TString data = GetReadResult(resRead);
        UNIT_ASSERT_VALUES_EQUAL(resRead.GetBatch(), 0);
        UNIT_ASSERT_EQUAL(data.size(), 0);
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

    bool ok = ProposeSchemaTx(runtime, sender, TTestSchema::CreateTableTxBody(tableId, testYdbSchema, testYdbPk),
                              NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));

    TString data = MakeTestBlob({0, 100}, testYdbSchema);
    UNIT_ASSERT(data.size() < NColumnShard::TLimits::MIN_BYTES_TO_INSERT);

    // Write into InsertTable
    auto writeIdOpt = WriteData(runtime, sender, longTxId, tableId, 1, data, testYdbSchema);
    UNIT_ASSERT(writeIdOpt);
    ProposeCommit(runtime, sender, ++txId, {*writeIdOpt});
    auto commitTxId = txId;

    // Drop table
    ok = ProposeSchemaTx(runtime, sender, TTestSchema::DropTableTxBody(tableId, 2), NOlap::TSnapshot(++planStep, ++txId));
    if (ok) {
        PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));
    }

    // Plan commit
    PlanCommit(runtime, sender, ++planStep, commitTxId);
}

void TestCompaction(std::optional<ui32> numWrites = {}) {
    TTestBasicRuntime runtime;
    TTester::Setup(runtime);

    TActorId sender = runtime.AllocateEdgeActor();
    CreateTestBootstrapper(runtime,
                        CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::ColumnShard),
                        &CreateColumnShard);

    TDispatchOptions options;
    options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvBoot));
    runtime.DispatchEvents(options);

    // Create table
    ui64 writeId = 0;
    ui64 tableId = 1;
    ui64 planStep = 100;
    ui64 txId = 100;

    bool ok = ProposeSchemaTx(runtime, sender, TTestSchema::CreateTableTxBody(tableId, testYdbSchema, testYdbPk),
                              NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));

    // Set tiering

    ui64 ts = 1620000000;
    TInstant now = TAppData::TimeProvider->Now();
    TDuration allow = TDuration::Seconds(now.Seconds() - ts + 3600);
    TDuration disallow = TDuration::Seconds(now.Seconds() - ts - 3600);

    TTestSchema::TTableSpecials spec;
    spec.SetTtlColumn("timestamp");
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("hot").SetTtlColumn("timestamp"));
    spec.Tiers.back().EvictAfter = disallow;
    spec.Tiers.emplace_back(TTestSchema::TStorageTier("cold").SetTtlColumn("timestamp"));
    spec.Tiers.back().EvictAfter = allow;
    spec.Tiers.back().S3 = TTestSchema::TStorageTier::FakeS3();

    ok = ProposeSchemaTx(runtime, sender, TTestSchema::AlterTableTxBody(tableId, 1, spec),
                            NOlap::TSnapshot(++planStep, ++txId));
    UNIT_ASSERT(ok);
    PlanSchemaTx(runtime, sender, NOlap::TSnapshot(planStep, txId));

    ProvideTieringSnapshot(runtime, sender, TTestSchema::BuildSnapshot(spec));

    // Writes

    std::vector<TString> blobs = MakeData({ts}, PORTION_ROWS, 0, spec.TtlColumn);
    const TString& triggerData = blobs[0];
    UNIT_ASSERT(triggerData.size() > NColumnShard::TLimits::MIN_BYTES_TO_INSERT);
    UNIT_ASSERT(triggerData.size() < NColumnShard::TLimits::GetMaxBlobSize());

    if (!numWrites) {
        numWrites = NOlap::TCompactionLimits().GranuleOverloadSize / triggerData.size();
    }

    ++planStep;
    ++txId;
    for (ui32 i = 0; i < *numWrites; ++i, ++writeId, ++planStep, ++txId) {
        std::vector<ui64> writeIds;
        UNIT_ASSERT(WriteData(runtime, sender, writeId, tableId, triggerData, testYdbSchema, true, &writeIds));

        ProposeCommit(runtime, sender, txId, writeIds);
        PlanCommit(runtime, sender, planStep, txId);

        if (i % 2 == 0) {
            TriggerTTL(runtime, sender, NOlap::TSnapshot(++planStep, ++txId), {}, 0, spec.TtlColumn);
        }
    }
}

}

namespace NColumnShard {
extern bool gAllowLogBatchingDefaultValue;
}

Y_UNIT_TEST_SUITE(TColumnShardTestSchema) {

    Y_UNIT_TEST(CreateTable) {
        ui64 tableId = 1;

        std::vector<TTypeId> intTypes = {
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
        std::vector<TTypeId> floatTypes = {
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

        std::vector<TTypeId> strTypes = {
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

        std::vector<TTypeId> xsonTypes = {
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
        TestTtl(false, false); // over NTypeIds::Timestamp ttl column
    }

    Y_UNIT_TEST(ExternalTTL_Types) {
        auto ydbSchema = testYdbSchema;
        for (auto typeId : {NTypeIds::Datetime, NTypeIds::Date, NTypeIds::Uint32, NTypeIds::Uint64}) {
            UNIT_ASSERT_EQUAL(ydbSchema[8].first, "saved_at");
            ydbSchema[8].second = TTypeInfo(typeId);

            TTestSchema::TTableSpecials specs;
            specs.SetTtlColumn("saved_at");

            TestTtl(false, false, specs, ydbSchema);
        }
    }

    Y_UNIT_TEST(RebootExternalTTL) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestTtl(true, false);
    }

    Y_UNIT_TEST(InternalTTL) {
        TestTtl(false, true); // over NTypeIds::Timestamp ttl column
    }

    Y_UNIT_TEST(InternalTTL_Types) {
        auto ydbSchema = testYdbSchema;
        for (auto typeId : {NTypeIds::Datetime, NTypeIds::Date, NTypeIds::Uint32, NTypeIds::Uint64}) {
            UNIT_ASSERT_EQUAL(ydbSchema[8].first, "saved_at");
            ydbSchema[8].second = TTypeInfo(typeId);

            TTestSchema::TTableSpecials specs;
            specs.SetTtlColumn("saved_at");

            TestTtl(false, true, specs, ydbSchema);
        }
    }

    Y_UNIT_TEST(RebootInternalTTL) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestTtl(true, true);
    }

    Y_UNIT_TEST(OneTier) {
        TTestSchema::TTableSpecials specs;
        specs.SetTtlColumn("timestamp");
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default").SetTtlColumn("timestamp"));
        TestTtl(false, true, specs);
    }

    Y_UNIT_TEST(RebootOneTier) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TTestSchema::TTableSpecials specs;
        specs.SetTtlColumn("timestamp");
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default").SetTtlColumn("timestamp"));
        TestTtl(true, true, specs);
    }

    Y_UNIT_TEST(OneTierExternalTtl) {
        TTestSchema::TTableSpecials specs;
        specs.SetTtlColumn("timestamp");
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default").SetTtlColumn("timestamp"));
        TestTtl(false, false, specs);
    }

    Y_UNIT_TEST(RebootOneTierExternalTtl) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TTestSchema::TTableSpecials specs;
        specs.SetTtlColumn("timestamp");
        specs.Tiers.emplace_back(TTestSchema::TStorageTier("default").SetTtlColumn("timestamp"));
        TestTtl(true, false, specs);
    }

    // TODO: EnableOneTierAfterTtl, EnableTtlAfterOneTier

    Y_UNIT_TEST(HotTiers) {
        TestTwoHotTiers(false, false);
    }

    Y_UNIT_TEST(RebootHotTiers) {
        TestTwoHotTiers(true, false);
    }

    Y_UNIT_TEST(HotTiersRevCompression) {
        TestTwoHotTiers(false, false, EInitialEviction::None, true);
    }

    Y_UNIT_TEST(RebootHotTiersRevCompression) {
        TestTwoHotTiers(true, false, EInitialEviction::None, true);
    }

    Y_UNIT_TEST(HotTiersTtl) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestTwoHotTiers(false, true);
    }

    Y_UNIT_TEST(RebootHotTiersTtl) {
        NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestTwoHotTiers(true, true);
    }

    Y_UNIT_TEST(HotTiersAfterTtl) {
        TestTwoHotTiers(false, false, EInitialEviction::Ttl);
    }

    Y_UNIT_TEST(RebootHotTiersAfterTtl) {
        TestTwoHotTiers(true, false, EInitialEviction::Ttl);
    }

    // TODO: EnableTtlAfterHotTiers

    Y_UNIT_TEST(ColdTiers) {
        TestHotAndColdTiers(false, EInitialEviction::Tiering);
    }

    Y_UNIT_TEST(RebootColdTiers) {
        //NColumnShard::gAllowLogBatchingDefaultValue = false;
        TestHotAndColdTiers(true, EInitialEviction::Tiering);
    }

    Y_UNIT_TEST(EnableColdTiersAfterNoEviction) {
        TestHotAndColdTiers(false, EInitialEviction::None);
    }

    Y_UNIT_TEST(RebootEnableColdTiersAfterNoEviction) {
        TestHotAndColdTiers(true, EInitialEviction::None);
    }

    Y_UNIT_TEST(EnableColdTiersAfterTtl) {
        TestHotAndColdTiers(false, EInitialEviction::Ttl);
    }

    Y_UNIT_TEST(RebootEnableColdTiersAfterTtl) {
        TestHotAndColdTiers(true, EInitialEviction::Ttl);
    }

    Y_UNIT_TEST(OneColdTier) {
        TestExport(false);
    }

    Y_UNIT_TEST(RebootOneColdTier) {
        TestExport(true);
    }

    Y_UNIT_TEST(ExportAfterFail) {
        TestExport(false, TExportTestOpts{.Misconfig = 1});
    }

    Y_UNIT_TEST(RebootExportAfterFail) {
        TestExport(true, TExportTestOpts{.Misconfig = 1});
    }

    Y_UNIT_TEST(ForgetAfterFail) {
        TestExport(false, TExportTestOpts{.Misconfig = 2});
    }

    Y_UNIT_TEST(RebootForgetAfterFail) {
        TestExport(true, TExportTestOpts{.Misconfig = 2});
    }

    Y_UNIT_TEST(ExportWithLostAnswer) {
        TestExport(false, TExportTestOpts{.Loss = 1});
    }

    Y_UNIT_TEST(RebootExportWithLostAnswer) {
        TestExport(true, TExportTestOpts{.Loss = 1});
    }

    Y_UNIT_TEST(ForgetWithLostAnswer) {
        TestExport(false, TExportTestOpts{.Loss = 2});
    }

    Y_UNIT_TEST(RebootForgetWithLostAnswer) {
        TestExport(true, TExportTestOpts{.Loss = 2});
    }
#if 0
    Y_UNIT_TEST(RebootReadNoTier) {
        TestExport(true, TExportTestOpts{.NoTier = 3});
    }
#endif
    // TODO: LastTierBorderIsTtl = false
    // TODO: AlterTierBorderAfterExport

    Y_UNIT_TEST(ColdCompactionSmoke) {
        TestCompaction();
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
