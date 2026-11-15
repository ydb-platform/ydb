#include "storage_pools.h"

NKikimr::TStoragePool::TStoragePool(const NKikimrStoragePool::TStoragePool &pool)
    : TBase(pool.GetName(), pool.GetKind())
{}

NKikimr::TStoragePool::operator NKikimrStoragePool::TStoragePool() const {
    NKikimrStoragePool::TStoragePool pool;
    pool.SetName(GetName());
    pool.SetKind(GetKind());
    return pool;
}

namespace NKikimr {

TStorageRoom::TStorageRoom(const NKikimrStorageSettings::TStorageRoom& room)
    : RoomId(room.GetRoomId())
{
    if (room.ChannelsSize() > 0) {
        for (auto& explanation: room.GetChannels()) {
            AssignChannel(explanation.GetPurpose(), explanation.GetChannel());
        }
    } else {
        // Fallback for old format
        for (auto& explanation: room.GetExplanation()) {
            AssignChannel(explanation.GetPurpose(), explanation.GetChannel());
        }
    }
}

void TStorageRoom::AssignChannel(EPurpose purpose, ui32 channel) {
    if (purpose == EPurpose::TChannelPurpose_EPurpose_External) {
        bool inserted = ExternalChannels.emplace(channel).second;
        Y_ABORT_UNLESS(inserted, "reassign is forbided, channel purpose was %s channel id was %d",
                 NKikimrStorageSettings::TChannelPurpose::EPurpose_Name(purpose).c_str(),
                 channel);
        return;
    }

    bool inserted = Purposes.emplace(purpose, channel).second;
    Y_ABORT_UNLESS(inserted, "reassign is forbided, channel purpose was %s channel id was %d",
             NKikimrStorageSettings::TChannelPurpose::EPurpose_Name(purpose).c_str(),
             channel);
}

ui32 TStorageRoom::GetChannel(EPurpose purpose, ui32 defaultChannel) const {
    auto it = Purposes.find(purpose);

    if (it == Purposes.end()) {
        return defaultChannel;
    }

    return it->second;
}

const TSet<ui32> TStorageRoom::GetExternalChannels(ui32 defaultChannel) const {
    if (ExternalChannels.empty()) {
        return {defaultChannel};
    }

    return ExternalChannels;
}


TStorageRoom::operator NKikimrStorageSettings::TStorageRoom() const {
    NKikimrStorageSettings::TStorageRoom room;
    room.SetRoomId(RoomId);
    for (auto& item: Purposes) {
        auto newLayout = room.AddChannels();
        newLayout->SetChannel(item.second);
        newLayout->SetPurpose(item.first);

        auto oldLayout = room.AddExplanation();
        oldLayout->SetChannel(item.second);
        oldLayout->SetPurpose(item.first);
    }
    if (!ExternalChannels.empty()) {
        const auto& externalChannel = ExternalChannels.begin();
        auto oldLayout = room.AddExplanation();
        oldLayout->SetChannel(*externalChannel);
        oldLayout->SetPurpose(EPurpose::TChannelPurpose_EPurpose_External);
    }
    for (auto& item: ExternalChannels) {
        auto newLayout = room.AddChannels();
        newLayout->SetChannel(item);
        newLayout->SetPurpose(EPurpose::TChannelPurpose_EPurpose_External);
    }
    return room;
}

} // namespace NKikimr
