#include "ut_helpers.h"

#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/utils/yql_panic.h>

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NYql::NDq {

using namespace NKikimr::NMiniKQL;

constexpr TDuration WaitTimeout = TDuration::Seconds(10);

namespace {
  void TestWriteBigBatch(bool isCloud) {
    const int batchSize = 7500;
    CleanupSolomon("cloudId1", "folderId1", "custom", isCloud);

    TFakeCASetup setup;
    InitAsyncOutput(setup, BuildSolomonShardSettings(isCloud));

    auto issue = setup.AsyncOutputPromises.Issue.GetFuture();
    setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory){
        TUnboxedValueBatch res;
        for (int i = 0; i < batchSize; i++) {
          res.emplace_back(CreateStruct(holderFactory, {
            NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(i + 200000)),
            NKikimr::NMiniKQL::MakeString(std::to_string(i)),
            NUdf::TUnboxedValuePod(678)
          }));
        }

        return res;
    });
    UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

    const auto metrics = GetSolomonMetrics("folderId1", "custom");
    UNIT_ASSERT_EQUAL(GetMetricsCount(metrics), batchSize);
  }
}

Y_UNIT_TEST_SUITE(TDqSolomonWriteActorTest) {
    Y_UNIT_TEST(TestWriteFormat) {
        CleanupSolomon("cloudId1", "folderId1", "custom", true);

        TFakeCASetup setup;
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto issue = setup.AsyncOutputPromises.Issue.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory){
            TUnboxedValueBatch res;
            res.emplace_back(CreateStruct(holderFactory, {
              NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(1624811684)),
              NKikimr::NMiniKQL::MakeString("123"),
              NUdf::TUnboxedValuePod(678)
            }));
            return res;
        });
        UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

        const auto metrics = GetSolomonMetrics("folderId1", "custom");
        const auto expected = R"([
  {
    "labels": [
      [
        "label1",
        "123"
      ],
      [
        "name",
        "sensor1"
      ]
    ],
    "ts": "1970-01-01T00:27:04.811684Z",
    "value": 678
  }
])";
        UNIT_ASSERT_STRINGS_EQUAL(metrics, expected);
    }

    Y_UNIT_TEST(TestWriteBigBatchMonitoring) {
      TestWriteBigBatch(true);
    }

    Y_UNIT_TEST(TestWriteBigBatchSolomon) {
      //TestWriteBigBatch(false);
    }

    Y_UNIT_TEST(TestWriteWithTimeseries) {
      const int batchSize = 10;
      CleanupSolomon("cloudId1", "folderId1", "custom", true);

      TFakeCASetup setup;
      InitAsyncOutput(setup, BuildSolomonShardSettings(true));

      auto issue = setup.AsyncOutputPromises.Issue.GetFuture();
      setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory){
          TUnboxedValueBatch res;

          for (int i = 0; i < batchSize; i++) {
            res.emplace_back(CreateStruct(holderFactory, {
              NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(i + 200000)),
              NKikimr::NMiniKQL::MakeString("123"),
              NUdf::TUnboxedValuePod(678)
            }));
          }

          return res;
      });
      UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

      const auto metrics = GetSolomonMetrics("folderId1", "custom");
      UNIT_ASSERT_EQUAL(GetMetricsCount(metrics), batchSize);
    }

    Y_UNIT_TEST(TestCheckpoints) {
      const int batchSize = 2400;

      {
        TFakeCASetup setup;
        CleanupSolomon("cloudId1", "folderId1", "custom", true);
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto stateSaved = setup.AsyncOutputPromises.StateSaved.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory){
            TUnboxedValueBatch res;

            for (int i = 0; i < batchSize; i++) {
              res.emplace_back(CreateStruct(holderFactory, {
                NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(i + 200000)),
                NKikimr::NMiniKQL::MakeString(std::to_string(i)),
                NUdf::TUnboxedValuePod(678)
              }));
            }

            return res;
        }, CreateCheckpoint(1));
        UNIT_ASSERT(stateSaved.Wait(WaitTimeout));

        const auto metrics = GetSolomonMetrics("folderId1", "custom");
        UNIT_ASSERT_EQUAL(GetMetricsCount(metrics), batchSize);
      }
    }

    Y_UNIT_TEST(TestShouldReturnAfterCheckpoint) {
      {
        TFakeCASetup setup;
        CleanupSolomon("cloudId1", "folderId1", "custom", true);
        InitAsyncOutput(setup, BuildSolomonShardSettings(true));

        auto stateSaved = setup.AsyncOutputPromises.StateSaved.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory){
            TUnboxedValueBatch res;
            res.emplace_back(CreateStruct(holderFactory, {
                NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(200000)),
                NKikimr::NMiniKQL::MakeString("abc"),
                NUdf::TUnboxedValuePod(678)
            }));
            return res;
        }, CreateCheckpoint(1));
        UNIT_ASSERT(stateSaved.Wait(WaitTimeout));

        auto issue = setup.AsyncOutputPromises.Issue.GetFuture();
        setup.AsyncOutputWrite([](NKikimr::NMiniKQL::THolderFactory& holderFactory){
            TUnboxedValueBatch res;
            res.emplace_back(CreateStruct(holderFactory, {
                NUdf::TUnboxedValuePod(static_cast<NUdf::TDataType<NUdf::TTimestamp>::TLayout>(200001)),
                NKikimr::NMiniKQL::MakeString("cba"),
                NUdf::TUnboxedValuePod(678)
              }));
              return res;
        });
        UNIT_ASSERT_C(!issue.Wait(WaitTimeout), issue.GetValue().ToString());

        const auto metrics = GetSolomonMetrics("folderId1", "custom");
        UNIT_ASSERT_EQUAL(GetMetricsCount(metrics), 2);
      }
    }
}

} // namespace NKikimr::NMiniKQL
