#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/protos/events.pb.h>

#include <google/protobuf/io/coded_stream.h>

#include <optional>

namespace NKikimr {

    // Accumulates complete tablets into a bounded response chunk while retaining
    // only one in-progress tablet and the small all-channel aggregate.
    class TLogoBlobIndexStatStreamAccumulator {
        class TChannelInfo {
        public:
            void Update(const TLogoBlobID& id, const TMemRecLogoBlob& memRec) {
                ++Count;
                DataSize += memRec.DataSize();
                MinId = Min(MinId, id);
                MaxId = Max(MaxId, id);
            }

            void Finish(NKikimrVDisk::ChannelInfo* output) const {
                output->set_count(Count);
                output->set_data_size(DataSize);
                output->set_min_id(MinId.ToString());
                output->set_max_id(MaxId.ToString());
            }

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
            void Update(const TLogoBlobID& id, const TMemRecLogoBlob& memRec) {
                const ui8 channel = id.Channel();
                if (channel >= Channels.size()) {
                    Channels.resize(channel + 1);
                }
                Channels[channel].Update(id, memRec);
            }

            void Finish(google::protobuf::RepeatedPtrField<NKikimrVDisk::ChannelInfo>* output) const {
                for (const TChannelInfo& channel : Channels) {
                    channel.Finish(output->Add());
                }
            }

        private:
            TVector<TChannelInfo> Channels;
        };

        class TTabletInfo {
        public:
            explicit TTabletInfo(ui64 tabletId)
                : TabletId(tabletId)
            {}

            void Update(const TLogoBlobID& id, const TMemRecLogoBlob& memRec) {
                Channels.Update(id, memRec);
            }

            void Finish(NKikimrVDisk::TabletInfo* output) const {
                output->set_tablet_id(TabletId);
                Channels.Finish(output->mutable_channels());
            }

            ui64 GetTabletId() const {
                return TabletId;
            }

        private:
            const ui64 TabletId;
            TAllChannels Channels;
        };

    public:
        explicit TLogoBlobIndexStatStreamAccumulator(ui64 maxChunkBytes)
            : MaxChunkBytes(Max<ui64>(maxChunkBytes, 1))
        {}

        void Update(const TKeyLogoBlob& key, const TMemRecLogoBlob& memRec) {
            Y_ABORT_UNLESS(!Finished);

            const TLogoBlobID id = key.LogoBlobID();
            if (!CurrentTablet || CurrentTablet->GetTabletId() != id.TabletID()) {
                AppendCurrentTablet();
                CurrentTablet.emplace(id.TabletID());
            }

            CurrentTablet->Update(id, memRec);
            AllChannels.Update(id, memRec);
        }

        bool IsChunkReady() const {
            return ChunkReady;
        }

        void ExtractChunk(NKikimrVDisk::LogoBlobIndexStat* output) {
            Y_ABORT_UNLESS(output);
            output->Clear();
            output->Swap(&Chunk);
            ChunkBytes = 0;
            ChunkReady = false;
        }

        void Finish() {
            Y_ABORT_UNLESS(!Finished);
            AppendCurrentTablet();
            AllChannels.Finish(Chunk.mutable_channels());
            Finished = true;
        }

    private:
        void AppendCurrentTablet() {
            if (!CurrentTablet) {
                return;
            }

            NKikimrVDisk::TabletInfo* output = Chunk.add_tablets();
            CurrentTablet->Finish(output);
            CurrentTablet.reset();

            const ui64 tabletSize = output->ByteSizeLong();
            // LogoBlobIndexStat.tablets is field 1, so its tag occupies one byte.
            ChunkBytes += 1 + google::protobuf::io::CodedOutputStream::VarintSize64(tabletSize) + tabletSize;
            ChunkReady = ChunkBytes >= MaxChunkBytes;
        }

    private:
        const ui64 MaxChunkBytes;
        ui64 ChunkBytes = 0;
        bool ChunkReady = false;
        bool Finished = false;
        std::optional<TTabletInfo> CurrentTablet;
        TAllChannels AllChannels;
        NKikimrVDisk::LogoBlobIndexStat Chunk;
    };

} // namespace NKikimr
