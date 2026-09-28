#include "query_statdb_stream.h"

#include <google/protobuf/io/coded_stream.h>

namespace NKikimr {

    void TLogoBlobIndexStatStreamAccumulator::TChannelInfo::Update(
            const TLogoBlobID& id,
            const TMemRecLogoBlob& memRec)
    {
        ++Count;
        DataSize += memRec.DataSize();
        MinId = Min(MinId, id);
        MaxId = Max(MaxId, id);
    }

    void TLogoBlobIndexStatStreamAccumulator::TChannelInfo::Finish(
            NKikimrVDisk::ChannelInfo* output) const
    {
        output->set_count(Count);
        output->set_data_size(DataSize);
        if (Count > 0) {
            output->set_min_id(MinId.ToString());
            output->set_max_id(MaxId.ToString());
        }
    }

    void TLogoBlobIndexStatStreamAccumulator::TAllChannels::Update(
            const TLogoBlobID& id,
            const TMemRecLogoBlob& memRec)
    {
        const ui8 channel = id.Channel();
        if (channel >= Channels.size()) {
            Channels.resize(channel + 1);
        }
        Channels[channel].Update(id, memRec);
    }

    void TLogoBlobIndexStatStreamAccumulator::TAllChannels::Finish(
            google::protobuf::RepeatedPtrField<NKikimrVDisk::ChannelInfo>* output) const
    {
        for (const TChannelInfo& channel : Channels) {
            channel.Finish(output->Add());
        }
    }

    TLogoBlobIndexStatStreamAccumulator::TTabletInfo::TTabletInfo(ui64 tabletId)
        : TabletId(tabletId)
    {}

    void TLogoBlobIndexStatStreamAccumulator::TTabletInfo::Update(
            const TLogoBlobID& id,
            const TMemRecLogoBlob& memRec)
    {
        Channels.Update(id, memRec);
    }

    void TLogoBlobIndexStatStreamAccumulator::TTabletInfo::Finish(
            NKikimrVDisk::TabletInfo* output) const
    {
        output->set_tablet_id(TabletId);
        Channels.Finish(output->mutable_channels());
    }

    ui64 TLogoBlobIndexStatStreamAccumulator::TTabletInfo::GetTabletId() const {
        return TabletId;
    }

    TLogoBlobIndexStatStreamAccumulator::TLogoBlobIndexStatStreamAccumulator(ui64 maxBatchBytes)
        : MaxBatchBytes(Max<ui64>(maxBatchBytes, 1))
    {}

    void TLogoBlobIndexStatStreamAccumulator::BeginKey(const TKeyLogoBlob&) {
    }

    void TLogoBlobIndexStatStreamAccumulator::UpdateFreshRecord(
            const TMemRecLogoBlob& memRec,
            const TRope*,
            const TKeyLogoBlob& key,
            ui64)
    {
        Update(key, memRec);
    }

    void TLogoBlobIndexStatStreamAccumulator::UpdateLevelRecord(
            const TMemRecLogoBlob& memRec,
            const TDiskPart*,
            const TKeyLogoBlob& key,
            ui64,
            const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>*)
    {
        Update(key, memRec);
    }

    void TLogoBlobIndexStatStreamAccumulator::FinishKey(const TKeyLogoBlob&) {
    }

    void TLogoBlobIndexStatStreamAccumulator::Update(
            const TKeyLogoBlob& key,
            const TMemRecLogoBlob& memRec)
    {
        Y_ABORT_UNLESS(!Finished);

        const TLogoBlobID id = key.LogoBlobID();
        if (!CurrentTablet || CurrentTablet->GetTabletId() != id.TabletID()) {
            AppendCurrentTablet();
            CurrentTablet.emplace(id.TabletID());
        }

        CurrentTablet->Update(id, memRec);
        AllChannels.Update(id, memRec);
    }

    bool TLogoBlobIndexStatStreamAccumulator::IsBatchReady() const {
        return BatchBytes >= MaxBatchBytes;
    }

    void TLogoBlobIndexStatStreamAccumulator::ExtractBatch(
            NKikimrVDisk::LogoBlobIndexStat* output)
    {
        Y_ABORT_UNLESS(output);
        output->Clear();
        output->Swap(&Batch);
        BatchBytes = 0;
    }

    void TLogoBlobIndexStatStreamAccumulator::Finish() {
        Y_ABORT_UNLESS(!Finished);
        AppendCurrentTablet();
        AllChannels.Finish(Batch.mutable_channels());
        Finished = true;
    }

    void TLogoBlobIndexStatStreamAccumulator::AppendCurrentTablet() {
        if (!CurrentTablet) {
            return;
        }

        NKikimrVDisk::TabletInfo* output = Batch.add_tablets();
        CurrentTablet->Finish(output);
        CurrentTablet.reset();

        const ui64 tabletSize = output->ByteSizeLong();
        // LogoBlobIndexStat.tablets is field 1, so its tag occupies one byte.
        BatchBytes += 1 + google::protobuf::io::CodedOutputStream::VarintSize64(tabletSize) + tabletSize;
    }

} // namespace NKikimr
