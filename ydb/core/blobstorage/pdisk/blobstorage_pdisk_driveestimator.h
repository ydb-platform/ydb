#pragma once

#include "defs.h"

#include "blobstorage_pdisk_blockdevice.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_drivemodel.h"
#include "blobstorage_pdisk_actorsystem_creator.h"
#include "blobstorage_pdisk_mon.h"

#include <util/system/hp_timer.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/core/base/resource_profile.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/base/domain.h>
#include <util/system/condvar.h>
#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {
namespace NPDisk {

class TDriveEstimator {
    TString Filename;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    TPDiskMon PDiskMon;
    std::unique_ptr<TActorSystemCreator> ActorSystemCreator;
    TActorSystem *ActorSystem;
    const ui32 QueueDepth;
    THolder<IBlockDevice> Device;
    ui64 DriveSize;
    THolder<TBufferPool> BufferPool;
    TBuffer::TPtr Buffer;
    TAtomic Counter = 0;
    TMutex Mtx;
    TCondVar CondVar;
    static constexpr ui32 Repeats = 32;
    static constexpr ui32 BufferSize = 128 << 20;
    static constexpr ui32 SectorSize = 4096;
    static constexpr ui32 SeekBufferSize = 4096;
    TVector<NHPTimer::STime> Durations;

    struct TLoadCompl : public NPDisk::TCompletionAction {
        TDriveEstimator *Estimator;

        TLoadCompl(TDriveEstimator *estimator);

        void Exec(TActorSystem *actorSystem) override;

        void Release(TActorSystem *actorSystem) override;
    };

    struct TSeekCompl : public NPDisk::TCompletionAction {
        TDriveEstimator *Estimator;
        ui32 Counter;
        NHPTimer::STime PrevCompletionTime;

        TSeekCompl(TDriveEstimator *estimator, ui32 counter, NHPTimer::STime prevCompletionTime);

        void Exec(TActorSystem *actorSystem) override;

        void Release(TActorSystem *actorSystem) override;
    };

    ui64 EstimateSeekTimeNs();

    void EstimateSpeed(const bool isAtDriveBegin, ui64 outSpeed[TDriveModel::OP_TYPE_COUNT]);

    ui64 MeasureOperationDuration(const ui32 type, const ui64 size);

    void EstimateGlueingDeadline(ui32 outGlueingDeadlin[TDriveModel::OP_TYPE_COUNT]);

    ui64 EstimateTrimSpeed();

public:
    TDriveEstimator(const TString filename);

    TDriveModel EstimateDriveModel();
};

}
}
