#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>

#include <optional>

namespace NKikimr {

    template <class TKey, class TMemRec>
    struct TLevelSegment;

    // Accumulates complete tablets into a bounded response batch while retaining
    // only one in-progress tablet and the small all-channel aggregate.
    class TLogoBlobIndexStatStreamAccumulator {
        class TChannelInfo {
        public:
            void Update(const TLogoBlobID& id, const TMemRecLogoBlob& memRec);
            void Finish(NKikimrVDisk::ChannelInfo* output) const;

        private:
            ui64 Count = 0;
            ui64 DataSize = 0;
            TLogoBlobID MinId = TLogoBlobID(
                Max<ui64>(), Max<ui32>(), Max<ui32>(), Max<ui8>(), 0, 0,
                TLogoBlobID::MaxPartId);
            TLogoBlobID MaxId;
        };

        class TAllChannels {
        public:
            void Update(const TLogoBlobID& id, const TMemRecLogoBlob& memRec);
            void Finish(google::protobuf::RepeatedPtrField<NKikimrVDisk::ChannelInfo>* output) const;

        private:
            TVector<TChannelInfo> Channels;
        };

        class TTabletInfo {
        public:
            explicit TTabletInfo(ui64 tabletId);

            void Update(const TLogoBlobID& id, const TMemRecLogoBlob& memRec);
            void Finish(NKikimrVDisk::TabletInfo* output) const;
            ui64 GetTabletId() const;

        private:
            const ui64 TabletId;
            TAllChannels Channels;
        };

    public:
        explicit TLogoBlobIndexStatStreamAccumulator(ui64 maxBatchBytes);

        void BeginKey(const TKeyLogoBlob&);

        void UpdateFreshRecord(
            const TMemRecLogoBlob& memRec,
            const TRope*,
            const TKeyLogoBlob& key,
            ui64);

        void UpdateLevelRecord(const TMemRecLogoBlob& memRec, const TDiskPart*,
                const TKeyLogoBlob& key, ui64,
                const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>*);

        void FinishKey(const TKeyLogoBlob&);

        void Update(const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec);
        bool IsBatchReady() const;
        void ExtractBatch(NKikimrVDisk::LogoBlobIndexStat* output);
        void Finish();

    private:
        void AppendCurrentTablet();

    private:
        const ui64 MaxBatchBytes;
        ui64 BatchBytes = 0;
        bool Finished = false;
        std::optional<TTabletInfo> CurrentTablet;
        TAllChannels AllChannels;
        NKikimrVDisk::LogoBlobIndexStat Batch;
    };

} // namespace NKikimr
