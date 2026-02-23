#include "defs.h"

#include <ydb/tools/stress_tool/proto/device_perf_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/message.h>
#include <util/system/tempfile.h>

#include "device_test_tool.h"
#include "device_test_tool_aio_test.h"
#include "device_test_tool_ddisk_test.h"
#include "device_test_tool_driveestimator.h"
#include "device_test_tool_pdisk_test.h"
#include "device_test_tool_pb_test.h"
#include "device_test_tool_trim_test.h"

namespace NKikimr {
namespace NPDisk {
    extern const ui64 YdbDefaultPDiskSequence = 0x7e5700007e570000;
}
}

constexpr i64 FileSize = 64ull << 30ull; // GiB
constexpr ui32 TestChunkSize = 32 << 20; // 32 MiB

using TPDiskTest32 = NKikimr::TPDiskTest<TestChunkSize>;
using TDDiskTest32 = NKikimr::TDDiskTest<TestChunkSize>;
using TPersistentBufferTest32 = NKikimr::TPersistentBufferTest<TestChunkSize>;

struct TPrinterStub : NKikimr::IResultPrinter {
    TVector<std::pair<TString, TString>> Results;
    bool ExpectResults;

    TPrinterStub(bool expectResults)
        : ExpectResults(expectResults)
    {}

    void AddResult(const TString& name, const TString& value) override {
        Results.emplace_back(name, value);
    }

    void AddGlobalParam(const TString&, const TString&) override {
    }

    void AddSpeedAndIops(const NKikimr::TSpeedAndIops&) override {
    }

    void SetTestType(const TString&) override {
    }

    void SetInFlight(ui32) override {
    }

    void PrintResults() override {
    }

    void EndTest() override {
        UNIT_ASSERT(!ExpectResults || Results.size() > 0);
    }
};

Y_UNIT_TEST_SUITE(TDeviceTestTool) {

template<typename P, typename T>
void ProbeTest(const TString &testDescription, bool expectResults, TMaybe<NKikimr::TResultPrinter::EOutputFormat> format = {}) {
    UNIT_ASSERT(!(expectResults && format));
    TTempFileHandle file;
    file.Resize(FileSize);
    NKikimr::TPerfTestConfig config(file.Name(), "name", "ROT", "json", "", true);

    P testProto;
    NProtoBuf::TextFormat::ParseFromString(testDescription, &testProto);

    THolder<NKikimr::TPerfTest> test(new T(config, testProto));
    TIntrusivePtr<NKikimr::IResultPrinter> printer;
    if (format) {
        printer = new NKikimr::TResultPrinter(*format);
    } else {
        printer = new TPrinterStub(expectResults);
    }

    test->SetPrinter(printer);
    test->RunTest();
    printer->EndTest();
}

Y_UNIT_TEST(AioTestRead) {
    TStringStream perfCfg;
    perfCfg << R"___(
        DurationSeconds: 10
        RequestSize: 4096
        QueueDepth: 8
        ReadProportion: 1
    )___";

    ProbeTest<NDevicePerfTest::TAioTest, NKikimr::TAioTest>(perfCfg.Str(), true);
}

Y_UNIT_TEST(AioTestWrite) {
    TStringStream perfCfg;
    perfCfg << R"___(
        DurationSeconds: 10
        RequestSize: 4096
        QueueDepth: 8
        ReadProportion: 0
    )___";

    ProbeTest<NDevicePerfTest::TAioTest, NKikimr::TAioTest>(perfCfg.Str(), true);
}

Y_UNIT_TEST(AioTestReadWrite) {
    TStringStream perfCfg;
    perfCfg << R"___(
        DurationSeconds: 10
        RequestSize: 4096
        QueueDepth: 8
        ReadProportion: 0.5
    )___";

    ProbeTest<NDevicePerfTest::TAioTest, NKikimr::TAioTest>(perfCfg.Str(), true);
}

constexpr ui32 TestDurationSec = NSan::PlainOrUnderSanitizer(30, 60);

TString GenReadCfg() {
    return TStringBuilder() << R"___(
        PDiskTestList: {
            PDiskReadLoad: {
                Tag: 1
                PDiskId: 1
                PDiskGuid: 12345
                VDiskId: {
                    GroupID: 1
                    GroupGeneration: 5
                    Ring: 1
                    Domain: 1
                    VDisk: 1
                }
                Chunks: { Slots: 1000 Weight: 1 }
                Chunks: { Slots: 1000 Weight: 1 }
                Chunks: { Slots: 1000 Weight: 1 }
                DurationSeconds: )___" << TestDurationSec << R"___(
                IntervalMsMin: 0
                IntervalMsMax: 0
                InFlightReads: 64
                Sequential: false
                IsWardenlessTest: true
            }
        }
        PDiskTestList: {
            PDiskReadLoad: {
                Tag: 1
                PDiskId: 1
                PDiskGuid: 12345
                VDiskId: {
                    GroupID: 1
                    GroupGeneration: 5
                    Ring: 1
                    Domain: 1
                    VDisk: 1
                }
                Chunks: { Slots: 1000 Weight: 1 }
                Chunks: { Slots: 1000 Weight: 1 }
                Chunks: { Slots: 1000 Weight: 1 }
                DurationSeconds: )___" << TestDurationSec << R"___(
                IntervalMsMin: 0
                IntervalMsMax: 0
                InFlightReads: 64
                Sequential: false
                IsWardenlessTest: true
            }
        }
    )___";
}

Y_UNIT_TEST(PDiskTestRead) {
    ProbeTest<NDevicePerfTest::TPDiskTest, TPDiskTest32>(GenReadCfg(), true);
}

Y_UNIT_TEST(PDiskTestReadAllPrinters) {
    for (auto f : { NKikimr::TResultPrinter::OUTPUT_FORMAT_WIKI,
                    NKikimr::TResultPrinter::OUTPUT_FORMAT_HUMAN,
                    NKikimr::TResultPrinter::OUTPUT_FORMAT_JSON}) {
        ProbeTest<NDevicePerfTest::TPDiskTest, TPDiskTest32>(GenReadCfg(), false, f);
    }
}

Y_UNIT_TEST(PDiskTestWrite) {
    TStringStream perfCfg;
    perfCfg << R"___(
        PDiskTestList: {
            PDiskReadLoad: {
                Tag: 3
                PDiskId: 1
                PDiskGuid: 12345
                VDiskId: {
                    GroupID: 1
                    GroupGeneration: 5
                    Ring: 1
                    Domain: 1
                    VDisk: 1
                }
                Chunks: { Slots: 64 Weight: 1 }
                Chunks: { Slots: 64 Weight: 1 }
                Chunks: { Slots: 64 Weight: 1 }
                DurationSeconds: )___" << TestDurationSec << R"___(
                IntervalMsMin: 0
                IntervalMsMax: 0
                InFlightReads: 64
                Sequential: false
                IsWardenlessTest: true
            }
        }
        PDiskTestList: {
            PDiskReadLoad: {
                Tag: 3
                PDiskId: 1
                PDiskGuid: 12345
                VDiskId: {
                    GroupID: 1
                    GroupGeneration: 5
                    Ring: 1
                    Domain: 1
                    VDisk: 1
                }
                Chunks: { Slots: 64 Weight: 1 }
                Chunks: { Slots: 64 Weight: 1 }
                Chunks: { Slots: 64 Weight: 1 }
                DurationSeconds: )___" << TestDurationSec << R"___(
                IntervalMsMin: 0
                IntervalMsMax: 0
                InFlightReads: 64
                Sequential: false
                IsWardenlessTest: true
            }
        }
    )___";

    ProbeTest<NDevicePerfTest::TPDiskTest, TPDiskTest32>(perfCfg.Str(), true);
}

Y_UNIT_TEST(DDiskTestWrite) {
    TStringStream perfCfg;
    perfCfg << R"___(
        DDiskTestList: {
            DDiskWriteLoad: {
                Tag: 4
                DDiskId: {
                    NodeId: 1
                    PDiskId: 1
                    DDiskSlotId: 1
                }
                Areas: { AreaSize: 10485760 Sequential: false }
                DurationSeconds: )___" << TestDurationSec << R"___(
                InFlightWrites: 64
                IntervalMsMin: 0
                IntervalMsMax: 0
                ExpectedChunkSize: 10485760
            }
        }
    )___";

    ProbeTest<NDevicePerfTest::TDDiskTest, TDDiskTest32>(perfCfg.Str(), true);
}

Y_UNIT_TEST(PersistentBufferTestWrite) {
    TStringStream perfCfg;
    perfCfg << R"___(
        PersistentBufferTestList: {
            PersistentBufferWriteLoad: {
                Tag: 4
                DDiskId: {
                    NodeId: 1
                    PDiskId: 1
                    DDiskSlotId: 1
                }
                WriteInfos: { Size: 4096 Weight: 1 }
                DurationSeconds: )___" << TestDurationSec << R"___(
                InFlightWrites: 64
            }
        }
    )___";

    ProbeTest<NDevicePerfTest::TPersistentBufferTest, TPersistentBufferTest32>(perfCfg.Str(), true);
}

Y_UNIT_TEST(PDiskTestLogWrite) {
    TStringStream perfCfg;
    perfCfg << R"___(
        PDiskTestList: {
            PDiskLogLoad: {
                Tag: 1
                PDiskId: 1
                PDiskGuid: 12345
                DurationSeconds: )___" << TestDurationSec << R"___(
                Workers: {
                    VDiskId: {GroupID: 1 GroupGeneration: 5 Ring: 1 Domain: 1 VDisk: 1}
                    MaxInFlight: 1
                    SizeIntervalMin: 65536
                    SizeIntervalMax: 65536
                    BurstInterval: 65536
                    BurstSize: 65536
                    StorageDuration: 1048576
                }
                Workers: {
                    VDiskId: {GroupID: 2 GroupGeneration: 5 Ring: 1 Domain: 1 VDisk: 1}
                    MaxInFlight: 1
                    SizeIntervalMin: 128
                    SizeIntervalMax: 128
                    BurstInterval: 2147483647
                    BurstSize: 1024
                    StorageDuration: 2147483647
                }
                IsWardenlessTest: true
            }
        }
    )___";

    ProbeTest<NDevicePerfTest::TPDiskTest, TPDiskTest32>(perfCfg.Str(), true);
}

Y_UNIT_TEST(DriveEstimatorTest) {
    ProbeTest<NDevicePerfTest::TDriveEstimatorTest, NKikimr::TDriveEstimatorTest>("", false);
}

}
