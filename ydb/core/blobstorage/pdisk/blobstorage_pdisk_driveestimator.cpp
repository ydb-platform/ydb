#include "blobstorage_pdisk_driveestimator.h"

#include <ydb/library/pdisk_io/aio.h>
#include <ydb/library/pdisk_io/wcache.h>

#include <util/system/align.h>

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////
// TLoadCompl
////////////////////////////////////////////////////////////////////////////////

TDriveEstimator::TLoadCompl::TLoadCompl(TDriveEstimator *estimator)
    : Estimator(estimator)
{}

void TDriveEstimator::TLoadCompl::Exec(TActorSystem *actorSystem) {
    Y_UNUSED(actorSystem);
    if (AtomicIncrement(Estimator->Counter) == Estimator->Repeats) {
        TGuard<TMutex> grd(Estimator->Mtx);
        if (AtomicGet(Estimator->Counter) == Estimator->Repeats) {
            Estimator->CondVar.Signal();
        }
    }
    delete this;
}

void TDriveEstimator::TLoadCompl::Release(TActorSystem *actorSystem) {
    Y_UNUSED(actorSystem);
    delete this;
}

////////////////////////////////////////////////////////////////////////////////
// TSeekCompl
////////////////////////////////////////////////////////////////////////////////

TDriveEstimator::TSeekCompl::TSeekCompl(TDriveEstimator *estimator, ui32 counter, NHPTimer::STime prevCompletionTime)
    : Estimator(estimator)
    , Counter(counter)
    , PrevCompletionTime(prevCompletionTime)
{}

void TDriveEstimator::TSeekCompl::Exec(TActorSystem *actorSystem) {
    Y_UNUSED(actorSystem);
    if (Counter == Estimator->Repeats) {
        TGuard<TMutex> grd(Estimator->Mtx);
        Estimator->CondVar.Signal();
    } else {
        const NHPTimer::STime now = HPNow();
        if (Counter > 1) {
            Estimator->Durations.push_back(now - PrevCompletionTime);
        }
        Estimator->Device->PwriteAsync(Estimator->Buffer->Data(), TDriveEstimator::SeekBufferSize,
                TDriveEstimator::SeekBufferSize * (Counter + 1), new TSeekCompl(Estimator, Counter + 1, now),
                TReqId(TReqId::EstimatorSeekCompl, 0), nullptr);
    }
    delete this;
}

void TDriveEstimator::TSeekCompl::Release(TActorSystem *actorSystem) {
    Y_UNUSED(actorSystem);
    delete this;
}

////////////////////////////////////////////////////////////////////////////////
// TDriveEstimator
////////////////////////////////////////////////////////////////////////////////

ui64 TDriveEstimator::EstimateSeekTimeNs() {
    TGuard<TMutex> grd(Mtx);
    Counter = 0;

    NHPTimer::STime start = HPNow();
    Device->PwriteAsync(Buffer->Data(), SeekBufferSize, 0, new TSeekCompl(this, 0, start),
            TReqId(TReqId::EstimatorSeekTimeNs, 0), nullptr);
    CondVar.WaitI(Mtx);
    grd.Release();

    NHPTimer::STime seekTime = Durations.front();
    for (ui32 i = 0; i < Durations.size(); ++i) {
        seekTime = Min(seekTime, Durations[i]);
    }
    Durations.clear();

    return HPNanoSeconds(seekTime);
}

void TDriveEstimator::EstimateSpeed(const bool isAtDriveBegin, ui64 outSpeed[TDriveModel::OP_TYPE_COUNT]) {
    NHPTimer::STime start = HPNow();
    constexpr ui64 operationSize = 1 << 20;
    static_assert(operationSize <= BufferSize, "operationSize must be less than or equal to BufferSize");
    TGuard<TMutex> grd(Mtx);
    Counter = 0;

    for (ui32 i = 0; i < Repeats; ++i) {
        ui64 offset;
        if (isAtDriveBegin) {
            offset = i * operationSize;
        } else {
            offset = DriveSize - (Repeats - i) * operationSize;
            offset = AlignDown<ui64>(offset, SectorSize);
        }
        Device->PwriteAsync(Buffer->Data(), operationSize, offset, new TLoadCompl(this),
                TReqId(TReqId::EstimatorSpeed1, 0), nullptr);
    }

    CondVar.WaitI(Mtx);

    double elapsed = HPSecondsFloat(HPNow() - start);
    outSpeed[TDriveModel::OP_TYPE_WRITE] = elapsed ? (double)operationSize * Repeats / elapsed : 0;

    Counter = 0;
    start = HPNow();

    for (ui32 i = 0; i < Repeats; ++i) {
        ui64 offset;
        if (isAtDriveBegin) {
            offset = i * operationSize;
        } else {
            offset = DriveSize - (Repeats - i) * operationSize;
            offset = AlignDown<ui64>(offset, SectorSize);
        }
        Device->PreadAsync(Buffer->Data(), operationSize, offset, new TLoadCompl(this),
                TReqId(TReqId::EstimatorSpeed2, 0), nullptr);
    }

    CondVar.WaitI(Mtx);

    elapsed = HPSecondsFloat(HPNow() - start);
    outSpeed[TDriveModel::OP_TYPE_READ] = elapsed ? (double)operationSize * Repeats / elapsed : 0;
    outSpeed[TDriveModel::OP_TYPE_AVG] = (outSpeed[TDriveModel::OP_TYPE_READ] + outSpeed[TDriveModel::OP_TYPE_WRITE]) / 2;
}

ui64 TDriveEstimator::EstimateTrimSpeed() {
    constexpr ui64 trimSize = 1ull << 20;
    static_assert(trimSize <= BufferSize, "trimSize must be less than or equal to BufferSize");
    TGuard<TMutex> grd(Mtx);
    Counter = 0;

    for (ui32 i = 0; i < Repeats; ++i) {
        Device->PwriteAsync(Buffer->Data(), trimSize, i * trimSize,
                new TLoadCompl(this), TReqId(TReqId::EstimatorTrimSpeed1, 0), nullptr);
    }
    CondVar.WaitI(Mtx);
    Counter = 0;

    NHPTimer::STime start = HPNow();
    for (ui32 i = 0; i < Repeats; ++i) {
        Device->TrimSync(trimSize, i * trimSize);
    }

    if (Device->GetIsTrimEnabled()) {
        const double elapsed = HPSecondsFloat(HPNow() - start);
        return (double)trimSize * Repeats / elapsed;
    } else {
        return 0;
    }
}

ui64 TDriveEstimator::MeasureOperationDuration(const ui32 type, const ui64 size) {
    constexpr ui32 eventsToSkip = Repeats / 8;

    TStackVec<TLoadCompl*, Repeats> completions;
    for (ui32 repeat = 0; repeat < Repeats; ++repeat) {
        completions.push_back(new TLoadCompl(this));
    }

    TGuard<TMutex> grd(Mtx);
    NHPTimer::STime start = HPNow();
    Counter = 0;
    for (ui32 repeat = 0; repeat < Repeats; ++repeat) {
        switch (type) {
            case TDriveModel::OP_TYPE_READ:
                Device->PreadAsync(Buffer->Data(), size, repeat * size, completions[repeat],
                        TReqId(TReqId::EstimatorDurationRead, 0), nullptr);
                break;
            case TDriveModel::OP_TYPE_WRITE:
                Device->PwriteAsync(Buffer->Data(), size, repeat * size, completions[repeat],
                        TReqId(TReqId::EstimatorDurationWrite, 0), nullptr);
                break;
            default:
                Y_ABORT();
        }
        if (repeat == eventsToSkip) {
            start = HPNow();
        }
    }
    NHPTimer::STime now = HPNow();
    CondVar.WaitI(Mtx);
    return HPNanoSeconds(now - start) / (Repeats - 1 - eventsToSkip);
}

void TDriveEstimator::EstimateGlueingDeadline(ui32 outGlueingDeadline[TDriveModel::OP_TYPE_COUNT]) {
    constexpr ui32 maxSize = 128u << 10;

    for (ui32 type = TDriveModel::OP_TYPE_READ; type <= TDriveModel::OP_TYPE_WRITE; ++type) {
        TStackVec<ui64, 32> durations;
        durations.push_back(Max<ui64>());
        for (ui32 sizeIdx = 1; sizeIdx <= maxSize / SeekBufferSize; ++sizeIdx) {
            const ui64 size = sizeIdx * SeekBufferSize;
            const ui64 durationNs = MeasureOperationDuration(type, size);
            durations.push_back(durationNs);
        }

        const ui64 minDuration = *MinElement(durations.begin(), durations.end());
        const ui64 threshold = minDuration * 2;
        ui32 lowerSize = SeekBufferSize;
        ui32 upperSize = SeekBufferSize;
        for (ui32 i = 1; i < durations.size(); ++i) {
            if (durations[i] < threshold) {
                if (lowerSize == SeekBufferSize) {
                    lowerSize = i * SeekBufferSize;
                }
                upperSize = i * SeekBufferSize;
            }
        }
        durations.clear();

        ui64 glueingDeadlineMin = Max<ui64>();
        for (ui32 size = lowerSize; size <= upperSize; size += 512) {
            const ui64 durationNs = MeasureOperationDuration(type, size);
            glueingDeadlineMin = Min(glueingDeadlineMin, durationNs);
        }
        outGlueingDeadline[type] = glueingDeadlineMin;
    }
    outGlueingDeadline[TDriveModel::OP_TYPE_AVG] = (outGlueingDeadline[TDriveModel::OP_TYPE_READ]
            + outGlueingDeadline[TDriveModel::OP_TYPE_WRITE]) / 2;
}

TDriveEstimator::TDriveEstimator(const TString filename)
    : Filename(filename)
    , Counters(new ::NMonitoring::TDynamicCounters())
    , PDiskMon(Counters, 0, nullptr)
    , ActorSystemCreator(new TActorSystemCreator)
    , ActorSystem(ActorSystemCreator->GetActorSystem())
    , QueueDepth(4)
    , Device(CreateRealBlockDevice(filename, 0, PDiskMon, 50, 0, QueueDepth, TDeviceMode::LockFile, 128, nullptr, nullptr))
    , BufferPool(CreateBufferPool(BufferSize, 1, false, {}))
    , Buffer(BufferPool->Pop())
{
    memset(Buffer->Data(), 7, Buffer->Size()); // Initialize the buffer so that Valgrind does not complain
    bool isBlockDevice = false;
    ActorSystem->AppData<TAppData>()->IoContextFactory->DetectFileParameters(filename, DriveSize, isBlockDevice);
    Y_ABORT_UNLESS(Buffer->Size() * Repeats < DriveSize);
    Device->Initialize(ActorSystem, {});
    Y_VERIFY_S(Device->IsGood(), "Cannot Initialize TBlockDevice");
}

TDriveModel TDriveEstimator::EstimateDriveModel() {
    TDriveModel model;
    for (ui32 type = TDriveModel::OP_TYPE_READ; type <= TDriveModel::OP_TYPE_AVG; ++type) {
        model.OptimalQueueDepth[type] = QueueDepth;
    }
    model.SeekTimeNsec = EstimateSeekTimeNs();
    ui64 speedBpsMin[TDriveModel::OP_TYPE_COUNT];
    ui64 speedBpsMax[TDriveModel::OP_TYPE_COUNT];
    EstimateSpeed(false, speedBpsMin);
    EstimateSpeed(true, speedBpsMax);

    for (ui32 type = TDriveModel::OP_TYPE_READ; type <= TDriveModel::OP_TYPE_AVG; ++type) {
        model.SpeedBpsMin[type] = Min(speedBpsMin[type], speedBpsMax[type]);
        model.SpeedBpsMax[type] = Max(speedBpsMin[type], speedBpsMax[type]);
        model.SpeedBps[type] = (model.SpeedBpsMax[type] + model.SpeedBpsMin[type]) / 2;
    }
    model.TrimSpeedBps = EstimateTrimSpeed();
    EstimateGlueingDeadline(model.GlueingDeadline);

    // Get the metadata
    TDriveData driveData = Device->GetDriveData();
    model.ModelSource = NKikimrBlobStorage::TDriveModel::SourceLocalMeasure;
    model.SourceSerialNumber = driveData.SerialNumber;
    model.SourceFirmwareRevision = driveData.FirmwareRevision;
    model.SourceModelNumber = driveData.ModelNumber;
    model.IsSourceSharedWithOs = false;  // TODO(cthulhu): obtain this data
    model.IsSourceWriteCacheEnabled = driveData.IsWriteCacheEnabled;

    return model;
}

}
}
