#pragma once

#include "defs.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk_driveestimator.h>
#include <util/system/hp_timer.h>

#include "device_test_tool.h"

namespace NKikimr {

class TDriveEstimatorTest : public TPerfTest {
    NPDisk::TDriveEstimator Estimator;
    NPDisk::TDriveModel Model;
    THPTimer Timer;

public:
    TDriveEstimatorTest(const TPerfTestConfig& cfg, const NDevicePerfTest::TDriveEstimatorTest& testProto)
        : TPerfTest(cfg)
        , Estimator(Cfg.Path)
    {
        Y_UNUSED(testProto);
    }

    void Init() override {
    }

    void Run() override {
        Timer.Reset();
        Model = Estimator.EstimateDriveModel();
    }

    void Finish() override {
        Cout << "Passed time# " << Timer.Passed() << " sec" << Endl;
        Cout << Model.ToString(true) << Endl;
    }

private:
    ~TDriveEstimatorTest() override {
    }
};

} // namespace NKikimr
