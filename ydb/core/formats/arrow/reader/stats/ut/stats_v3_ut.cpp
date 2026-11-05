#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/formats/arrow/reader/stats/stats.h>

#include <limits>

namespace NKikimr::NScanStats {
namespace {
using TStorage = NKqpProto::TColumnShardStepDetails::TStorageRead;

const NKqpProto::TColumnShardDataSourceRead &
Source(const NKqpProto::TColumnShardScanStats &report, TStringBuf kind) {
  for (const auto &read : report.GetStorageReads()) {
    if (read.GetSourceKind() == kind) {
      return read;
    }
  }
  UNIT_FAIL("Missing source " << kind);
  return report.GetStorageReads(0);
}

std::shared_ptr<TOperation>
Backend(const std::shared_ptr<TCollector> &collector,
        const std::shared_ptr<TOperation> &parent = {}) {
  auto operation = collector->Start(TStep::StorageRead, "STORAGE_READ",
                                    "BlobStorage", parent);
  operation->Update([](auto &record) {
    auto *details = record.MutableDetails()->MutableStorageRead();
    details->SetBackendKind(TStorage::BlobStorage);
    details->SetStorageTier("default");
    details->SetDataKind(TStorage::data);
  });
  return operation;
}
} // namespace

Y_UNIT_TEST_SUITE(TColumnShardStorageV3) {
  Y_UNIT_TEST(BackendChunksAreOneAttemptAndCacheHitIsSeparateOrigin) {
    auto collector = std::make_shared<TCollector>(1);
    auto operation = Backend(collector);
    operation->AddReadBytes(30);
    operation->AddReadBytes(70);
    operation->Finish();
    RecordDataSourceRead("BlobCache", "default", 20, TStorage::data,
                         TCapture({}, collector));
    const auto report = collector->Drain(true);
    UNIT_ASSERT(report->GetFinished());
    UNIT_ASSERT_VALUES_EQUAL(report->GetStorageStatsVersion(), 1);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadBytes(),
                             100);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobCache").GetReadBytes(), 20);
    UNIT_ASSERT_VALUES_EQUAL(report->GetSteps(0).DataSourceReadsSize(), 1);
  }

  Y_UNIT_TEST(SameQuerySharedDeliveryDoesNotChargeOriginTwice) {
    auto collector = std::make_shared<TCollector>(1);
    auto first = collector->Start(TStep::FetchColumns, "first", "first");
    auto second = collector->Start(TStep::FetchColumns, "second", "second");
    auto operation = Backend(collector, first);
    operation->AddReadBytes(100);
    operation->Finish();
    RecordNonOwnerDataSourceRead("BlobStorage", "default", TStorage::data,
                                 TCapture(first, collector), operation);
    RecordNonOwnerDataSourceRead("BlobStorage", "default", TStorage::data,
                                 TCapture(second, collector), operation);
    first->Finish();
    second->Finish();
    const auto report = collector->Drain(true);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadBytes(),
                             100);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(
        Source(*report, "BlobStorage").GetUnknownByteReads(), 0);
    ui64 references = 0;
    for (const auto &step : report->GetSteps()) {
      if (step.GetStepName() == "SHARED_READ") {
        ++references;
        UNIT_ASSERT(step.GetDataSourceReads(0).GetShared());
        UNIT_ASSERT_VALUES_EQUAL(
            step.GetDataSourceReads(0).GetUnknownByteReads(), 1);
      }
    }
    UNIT_ASSERT_VALUES_EQUAL(references, 1);
  }

  Y_UNIT_TEST(CrossQuerySharedDeliveryIsUnknownInsteadOfZeroCost) {
    auto owner = std::make_shared<TCollector>(1);
    auto consumer = std::make_shared<TCollector>(1);
    auto operation = Backend(owner);
    operation->AddReadBytes(100);
    operation->Finish();
    RecordNonOwnerDataSourceRead("BlobStorage", "default", TStorage::data,
                                 TCapture({}, consumer), operation);
    const auto report = consumer->Drain(true);
    UNIT_ASSERT(report->GetFinished());
    UNIT_ASSERT(Source(*report, "BlobStorage").GetShared());
    UNIT_ASSERT_VALUES_EQUAL(
        Source(*report, "BlobStorage").GetUnknownByteReads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadBytes(), 0);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadCount(), 1);
    UNIT_ASSERT_VALUES_EQUAL(
        Source(*owner->Drain(true), "BlobStorage").GetReadBytes(), 100);
  }

  Y_UNIT_TEST(FailedAttemptWithoutPayloadIsNotMeasuredZero) {
    auto collector = std::make_shared<TCollector>(1);
    auto operation = Backend(collector);
    operation->Finish(TStep::failed);
    const auto report = collector->Drain(true);
    UNIT_ASSERT_VALUES_EQUAL(
        Source(*report, "BlobStorage").GetUnknownByteReads(), 1);
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "BlobStorage").GetReadCount(), 1);
  }

  Y_UNIT_TEST(CumulativeSnapshotsKeepSourcesAndTableIdentity) {
    auto collector = std::make_shared<TCollector>(1);
    collector->SetTablePath("/Root/table");
    auto operation = Backend(collector);
    operation->AddReadBytes(40);
    operation->Finish();
    const auto first = collector->Drain(false);
    RecordDataSourceRead("SystemView", "view", 12, TStorage::data,
                         TCapture({}, collector));
    const auto second = collector->Drain(true);
    UNIT_ASSERT_VALUES_EQUAL(Source(*first, "BlobStorage").GetReadBytes(), 40);
    UNIT_ASSERT_VALUES_EQUAL(Source(*second, "BlobStorage").GetReadBytes(), 40);
    UNIT_ASSERT_VALUES_EQUAL(Source(*second, "SystemView").GetReadBytes(), 12);
    UNIT_ASSERT_STRINGS_EQUAL(second->GetTablePath(), "/Root/table");
    UNIT_ASSERT_VALUES_EQUAL(second->StepsSize(), 1);
    UNIT_ASSERT(collector->Drain(true)->HasStorageStatsVersion());
  }

  Y_UNIT_TEST(SourceBucketsUnionDataKindAndDoNotWrap) {
    auto collector = std::make_shared<TCollector>(1);
    collector->AddStorageRead("BlobStorage", "default", 10, TStorage::data);
    collector->AddStorageRead("BlobStorage", "default", 20, TStorage::index);
    const auto mixed = collector->Drain(true);
    UNIT_ASSERT_VALUES_EQUAL(mixed->StorageReadsSize(), 1);
    UNIT_ASSERT_STRINGS_EQUAL(Source(*mixed, "BlobStorage").GetDataKind(),
                              "mixed");
    collector->AddStorageRead("BlobStorage", "default",
                              std::numeric_limits<ui64>::max(), TStorage::data);
    const auto overflow = collector->Drain(true);
    UNIT_ASSERT(overflow->GetTruncated());
    UNIT_ASSERT_VALUES_EQUAL(Source(*overflow, "BlobStorage").GetReadBytes(),
                             std::numeric_limits<ui64>::max());
    UNIT_ASSERT_VALUES_EQUAL(
        Source(*overflow, "BlobStorage").GetUnknownByteReads(), 1);
  }

  Y_UNIT_TEST(DirectSourcesAreNotInheritedButIndexContextSurvivesFinish) {
    auto collector = std::make_shared<TCollector>(1);
    auto parent = collector->Start(TStep::Reader, "parent", "parent");
    parent->Update([](auto &record) {
      record.SetMetaStep("IndexFilter");
      record.SetCanonicalPath("");
      record.SetApplicationOrder(2);
      record.SetSharedIndexInputs(true);
      auto *descriptor = record.AddIndexDescriptors();
      descriptor->SetIndexId(17);
      descriptor->SetName("bloom");
      descriptor->SetType("BLOOM_FILTER");
      descriptor->AddColumns("value");
    });
    parent->AddDataSourceRead("Memory", "InlineIndex", 8, TStorage::index);
    parent->Finish();
    auto child = collector->Start(TStep::Decode, "child", "child", parent);
    child->Finish();
    const auto report = collector->Drain(true);
    const auto &record = report->GetSteps(1);
    UNIT_ASSERT_VALUES_EQUAL(record.DataSourceReadsSize(), 0);
    UNIT_ASSERT_VALUES_EQUAL(record.IndexDescriptorsSize(), 1);
    UNIT_ASSERT_VALUES_EQUAL(record.GetIndexDescriptors(0).GetIndexId(), 17);
    UNIT_ASSERT_VALUES_EQUAL(record.GetApplicationOrder(), 2);
    UNIT_ASSERT(!record.HasSharedIndexInputs());
    UNIT_ASSERT_VALUES_EQUAL(Source(*report, "Memory").GetReadBytes(), 8);
  }
}
} // namespace NKikimr::NScanStats
