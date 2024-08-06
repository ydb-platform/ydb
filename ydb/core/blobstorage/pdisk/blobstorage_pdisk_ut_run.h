#pragma once
#include "defs.h"

#include "blobstorage_pdisk_ut_config.h"
#include "blobstorage_pdisk_ut_context.h"

#include <ydb/core/base/services/blobstorage_service_id.h>

#include <util/folder/tempdir.h>

namespace NKikimr {

struct TTestRunConfig {
    TTestRunConfig(TTestContext *testContext)
        : TestContext(testContext)
    {}

    TTestContext *TestContext;
    ui32 Instances = 1;
    ui32 ChunkSize = 128 << 20;
    bool IsBad = false;
    bool IsErasureEncodeUserLog = false;
    ui32 BeforeTestSleepMs = 100;
};

void Run(TVector<IActor*> tests, TTestRunConfig runCfg);

template <class T>
void Run(TTestRunConfig runCfg) {
    const TActorId pDiskId = MakeBlobStoragePDiskID(1, 1);
    TVector<IActor*> tests;
    for (ui32 i = 0; i < runCfg.Instances; ++i) {
        TIntrusivePtr<TTestConfig> testConfig = new TTestConfig(TVDiskID(0, 1, 0, 0, i), pDiskId);
        tests.push_back(new T(testConfig.Get()));
    }
    Run(tests, runCfg);
}

template <class T>
static void Run(TTestContext *tc, ui32 instances = 1, ui32 chunkSize = 128 << 20, bool isBad = false,
        TString = TString(), ui32 beforeTestSleepMs = 100) {

    TTestRunConfig cfg(tc);
    cfg.Instances = instances;
    cfg.ChunkSize = chunkSize;
    cfg.IsBad = isBad;
    cfg.IsErasureEncodeUserLog = false;
    cfg.BeforeTestSleepMs = beforeTestSleepMs;

    Run<T>(cfg);
}

} // NKikimr
