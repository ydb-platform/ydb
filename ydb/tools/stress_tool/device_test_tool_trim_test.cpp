#include "device_test_tool_trim_test.h"

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_mon.h>

#include <ydb/library/pdisk_io/file_params.h>
#include <util/generic/string.h>
#include <util/random/entropy.h>
#include <util/random/mersenne64.h>
#include <util/stream/format.h>
#include <util/string/printf.h>

#ifdef _linux_
#   include <sys/ioctl.h>
#   include <linux/fs.h>
#endif

namespace NKikimr {

TTrimTest::TTrimTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TTrimTest& testProto)
    : TPerfTest(cfg)
    , Size(testProto.GetRequestSize() != 0 ? testProto.GetRequestSize() : (2 << 20))
    , DurationSec(testProto.GetDurationSeconds() != 0 ? testProto.GetDurationSeconds() : 60)
    , Buffer(Size)
{}

void TTrimTest::Init() {
    if (Cfg.DeviceType == NPDisk::EDeviceType::DEVICE_TYPE_ROT) {
        return;
    }

    bool isBlockDevice = false;
    DetectFileParameters(Cfg.Path, DeviceSizeBytes, isBlockDevice);

    File = MakeHolder<TFileHandle>(Cfg.Path.c_str(), OpenExisting | RdWr | DirectAligned | Sync);

    int ret = File->Flock(LOCK_EX | LOCK_NB);
    Y_VERIFY_S(ret == 0, "Error in file locking, path# " << Cfg.Path.Quote());
    NPrivate::TMersenne64 randGen(Seed());
    ui64 *buff64 = (ui64*)Buffer.Get();
    ui32 size64 = Buffer.Size() / sizeof(ui64);
    for (ui32 i = 0; i < size64; ++i) {
        buff64[i] = randGen.GenRand();
    }
}

void TTrimTest::Run() {
    if (Cfg.DeviceType == NPDisk::EDeviceType::DEVICE_TYPE_ROT) {
        return;
    }

    THPTimer timer;
    EventsDone = 0;
    TrimTime = 0;
    timer.Reset();
    NPrivate::TMersenne64 randGen(Seed());
    while (timer.Passed() < DurationSec) {
        ui64 offset = randGen.GenRand() % (DeviceSizeBytes - Size);
        offset = (offset / 512) * 512;
        ui64 range[2] = {offset, Size};
        File->Pwrite(Buffer.Get(), Size, offset);
        NHPTimer::STime start = HPNow();
#ifdef _linux_
        if (ioctl((FHANDLE)*File, BLKDISCARD, &range)) {
            Cerr << "Error during BLKDISCARD, strerror# " << strerror(errno) << Endl;
            return;
        }
#else
        Y_UNUSED(range);
        Cerr << "This platform is not support trim test" << Endl;
        return;
#endif
        NHPTimer::STime reqDuration = HPNow() - start;
        TrimTime += reqDuration;
        LatencyUs.Increment(HPMicroSeconds(reqDuration));
        ++EventsDone;
    }
    PrintReport();
}

void TTrimTest::Finish() {
    if (Cfg.DeviceType == NPDisk::EDeviceType::DEVICE_TYPE_ROT) {
        return;
    }

    int ret = File->Flock(LOCK_UN);
    Y_VERIFY_S(ret == 0, "Error in file unlocking, path# " << Cfg.Path.Quote());
}

void TTrimTest::PrintReport() {
    double TrimTimeSec = Max(1.0, HPSecondsFloat(TrimTime));
    Printer->AddResult("Size", ToString(HumanReadableSize(Size, SF_BYTES)));
    Printer->AddResult("Duration,sec", DurationSec);
    Printer->AddResult("Trims done", EventsDone);
    Printer->AddResult("Speed", Sprintf("%.1f MB/s", double(EventsDone * Size) / 1e6 / TrimTimeSec));
    Printer->AddResult("IOPS", Sprintf("%.2f", EventsDone / TrimTimeSec));
    for (float percentile : {1., 0.999, 0.99, 0.9, 0.5, 0.1}) {
        TString perc_name = Sprintf("%.1f perc", percentile * 100);
        Printer->AddResult(perc_name, Sprintf("%lu us", LatencyUs.GetPercentile(percentile)));
    }
    Printer->PrintResults();
}

TTrimTest::~TTrimTest() {
}

} // namespace NKikimr
