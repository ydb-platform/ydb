#pragma once

#include <ydb/core/protos/bind_channel_storage_pool.pb.h>
#include <ydb/core/protos/channel_purpose.pb.h>

#include <util/system/types.h>
#include <util/generic/vector.h>
#include <util/generic/map.h>

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

public:
    TStorageRoom(ui32 id)
        : RoomId(id)
    {}

    TStorageRoom(const NKikimrStorageSettings::TStorageRoom& room)
        : RoomId(room.GetRoomId())
    {
        for (auto& explanation: room.GetExplanation()) {
            AssignChannel(explanation.GetPurpose(), explanation.GetChannel());
        }
    }

    void AssignChannel(EPurpose purpose, ui32 channel) {
        bool repeated = Purposes.emplace(purpose, channel).second;
        Y_VERIFY(repeated, "reassign is forbided, channle purpose was %s chennel id was %d",
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

    operator NKikimrStorageSettings::TStorageRoom() const {
        NKikimrStorageSettings::TStorageRoom room;
        room.SetRoomId(RoomId);
        for (auto& item: Purposes) {
            auto layout = room.AddExplanation();
            layout->SetChannel(item.second);
            layout->SetPurpose(item.first);
        }
        return room;
    }

    ui32 GetId() const {
        return RoomId;
    }

    operator bool() const {
        return !!Purposes;
    }
};

}
