#pragma once

#include "defs.h"

#include <ydb/library/pdisk_io/buffers.h>
#include <library/cpp/monlib/dynamic_counters/percentile/percentile_lg.h>

#include <util/system/hp_timer.h>

#include "device_test_tool.h"

namespace NKikimr {

class TTrimTest : public TPerfTest {
    THolder<TFileHandle> File;
    ui64 Size;
    const double DurationSec;
    NPDisk::TAlignedData Buffer;
    ui64 DeviceSizeBytes;
    ui64 EventsDone;
    NHPTimer::STime TrimTime;
    NMonitoring::TPercentileTrackerLg<10, 5, 1> LatencyUs;

public:
    TTrimTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TTrimTest& testProto);
    void Init() override;
    void Run() override;
    void Finish() override;

private:
    void PrintReport();
    ~TTrimTest() override;
};

} // namespace NKikimr
