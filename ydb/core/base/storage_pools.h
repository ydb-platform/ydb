#pragma once

#include <ydb/core/protos/bind_channel_storage_pool.pb.h>
#include <ydb/core/protos/channel_purpose.pb.h>

#include <util/system/types.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>
#include <util/generic/set.h>

namespace NKikimr {

struct TStoragePool: public std::pair<TString, TString> {
    using TBase = std::pair<TString, TString>;

    using TBase::TBase;
    TStoragePool(const NKikimrStoragePool::TStoragePool& pool);

    const TString& GetName() const {
        return first;
    }

    const TString& GetKind() const {
        return second;
    }

    operator NKikimrStoragePool::TStoragePool() const;
};

using TStoragePools = TVector<TStoragePool>;

using TChannelBind = NKikimrStoragePool::TChannelBind;

using TChannelsBindings = TVector<TChannelBind>;

struct TStorageRoom: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<TStorageRoom>;
    using EPurpose = NKikimrStorageSettings::TChannelPurpose::EPurpose;

private:
    ui32 RoomId;
    TMap<EPurpose, ui32> Purposes;
    TSet<ui32> ExternalChannels;

public:
    TStorageRoom(ui32 id)
        : RoomId(id)
    {}

    TStorageRoom(const NKikimrStorageSettings::TStorageRoom& room)
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

    void AssignChannel(EPurpose purpose, ui32 channel) {
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

    ui32 GetChannel(EPurpose purpose, ui32 defaultChannel) const {
        auto it = Purposes.find(purpose);

        if (it == Purposes.end()) {
            return defaultChannel;
        }

        return it->second;
    }

    const TSet<ui32> GetExternalChannels(ui32 defaultChannel) const {
        if (ExternalChannels.empty()) {
            return {defaultChannel};
        }

        return ExternalChannels;
    }

    operator NKikimrStorageSettings::TStorageRoom() const {
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

    ui32 GetId() const {
        return RoomId;
    }

    operator bool() const {
        return !!Purposes || !!ExternalChannels;
    }
};

}
