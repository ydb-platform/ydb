#pragma once
#include <ydb/library/accessor/accessor.h>
#include <util/system/types.h>
#include <util/generic/string.h>

namespace NKikimr::NOlap::NBlobOperations::NBlobStorage {
class TBlobAddress {
private:
    YDB_READONLY(ui32, GroupId, 0);
    YDB_READONLY(ui32, ChannelId, 0);
public:
    TBlobAddress(const ui32 groupId, const ui32 channelId)
        : GroupId(groupId)
        , ChannelId(channelId) {

    }

    TString DebugString() const;

    explicit operator size_t() const {
        return GroupId << 32 + ChannelId;
    }

    bool operator==(const TBlobAddress& item) const {
        return GroupId == item.GroupId && ChannelId == item.ChannelId;
    }
};
}