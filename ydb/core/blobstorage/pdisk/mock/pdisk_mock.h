#pragma once

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/util/interval_set.h>
#include <ydb/library/actors/core/actor.h>
#include <util/generic/ptr.h>

namespace NKikimr {

    enum EPDiskMockEvents {
        EvBecomeError = TEvBlobStorage::EvEnd + 1,
        EvBecomeNormal,
        EvReplaceData,
    };

    class TPDiskMockState : public TThrRefBase {
        struct TImpl;
        std::unique_ptr<TImpl> Impl;
        friend class TPDiskMockActor;

    public:
        using TPtr = TIntrusivePtr<TPDiskMockState>;

    public:
        TPDiskMockState(ui32 nodeId, ui32 pdiskId, ui64 pdiskGuid, ui64 size, ui32 chunkSize = 128 << 20,
                bool isDiskReadOnly = false, NPDisk::EDeviceType deviceType = NPDisk::EDeviceType::DEVICE_TYPE_NVME);
        TPDiskMockState(std::unique_ptr<TImpl>&& impl);
        ~TPDiskMockState();

        void SetCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end, bool enabled);
        bool HasCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end);
        std::set<ui32> GetChunks();
        TMaybe<NPDisk::TOwnerRound> GetOwnerRound(const TVDiskID& vDiskId) const;
        ui32 GetChunkSize() const;
        TIntervalSet<i64> GetWrittenAreas(ui32 chunkIdx) const;
        void TrimQuery();
        void SetStatusFlags(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor);
        void SetStatusFlags(NPDisk::TStatusFlags flags);
        TString& GetStateErrorReason();

        TPtr Snapshot(); // create a copy of PDisk whole state

        void SetReadOnly(const TVDiskID& vDiskId, bool isReadOnly);

        bool IsDiskReadOnly() const;
    };

    struct TEvMoveDrive : TEventLocal<TEvMoveDrive, EvReplaceData> {
        TIntrusivePtr<TPDiskMockState>& State;

        TEvMoveDrive(TIntrusivePtr<TPDiskMockState>& state)
            : State(state)
        {}
    };

    IActor *CreatePDiskMockActor(TPDiskMockState::TPtr state);

} // NKikimr
