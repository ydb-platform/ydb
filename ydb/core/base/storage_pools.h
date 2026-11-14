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

    TStorageRoom(const NKikimrStorageSettings::TStorageRoom& room);

    void AssignChannel(EPurpose purpose, ui32 channel);

    ui32 GetChannel(EPurpose purpose, ui32 defaultChannel) const;

    const TSet<ui32> GetExternalChannels(ui32 defaultChannel) const;

    operator NKikimrStorageSettings::TStorageRoom() const;

    ui32 GetId() const {
        return RoomId;
    }

    operator bool() const {
        return !!Purposes || !!ExternalChannels;
    }
};

}
