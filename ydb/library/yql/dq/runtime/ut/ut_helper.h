#pragma once

#include <ydb/library/yql/dq/runtime/dq_channel_storage.h> 

#include <util/generic/hash.h>

namespace NYql::NDq {


class TMockChannelStorage : public IDqChannelStorage {
public:
    TMockChannelStorage(ui64 capacity)
        : Capacity(capacity) {}

    bool IsEmpty() const override {
        return Blobs.empty();
    }

    bool IsFull() const override {
        return Capacity <= UsedSpace;
    }

    void Put(ui64 blobId, TBuffer&& blob) override {
        if (UsedSpace + blob.size() > Capacity) {
            ythrow yexception() << "Space limit exceeded";
        }

        auto result = Blobs.emplace(blobId, std::move(blob));
        Y_VERIFY(result.second);
        UsedSpace += result.first->second.size();
    }

    bool Get(ui64 blobId, TBuffer& data) override {
        if (!Blobs.contains(blobId)) {
            ythrow yexception() << "Not found";
        }

        if (GetBlankRequests) {
            --GetBlankRequests;
            return false;
        }

        data = std::move(Blobs[blobId]);
        Blobs.erase(blobId);
        UsedSpace -= data.size();

        return true;
    }

public:
    void SetBlankGetRequests(ui32 count) {
        GetBlankRequests = count;
    }

private:
    const ui64 Capacity;
    THashMap<ui64, TBuffer> Blobs;
    ui64 UsedSpace = 0;
    ui32 GetBlankRequests = 0;
};

} // namespace NYql::NDq
