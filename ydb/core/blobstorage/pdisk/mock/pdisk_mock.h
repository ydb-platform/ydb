#pragma once

#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/util/interval_set.h>
#include <library/cpp/actors/core/actor.h>
#include <util/generic/ptr.h>

namespace NKikimr {

    class TPDiskMockState : public TThrRefBase {
        struct TImpl;
        std::unique_ptr<TImpl> Impl;
        friend class TPDiskMockActor;

    public:
        using TPtr = TIntrusivePtr<TPDiskMockState>;

    public:
        TPDiskMockState(ui32 nodeId, ui32 pdiskId, ui64 pdiskGuid, ui64 size, ui32 chunkSize = 128 << 20);
        TPDiskMockState(std::unique_ptr<TImpl>&& impl);
        ~TPDiskMockState();

        void SetCorruptedArea(ui32 chunkIdx, ui32 begin, ui32 end, bool enabled);
        std::set<ui32> GetChunks();
        ui32 GetChunkSize() const;
        TIntervalSet<i64> GetWrittenAreas(ui32 chunkIdx) const;
        void TrimQuery();
        void SetStatusFlags(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor);
        void SetStatusFlags(NPDisk::TStatusFlags flags);

        TPtr Snapshot(); // create a copy of PDisk whole state
    };

    IActor *CreatePDiskMockActor(TPDiskMockState::TPtr state);

} // NKikimr
