#pragma once

#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>

#include <util/generic/hash.h>

namespace NYql::NDq {


class TMockChannelStorage : public IDqChannelStorage {
public:
    TMockChannelStorage(ui64 capacity)
        : Capacity(capacity) {}

    bool IsEmpty() override {
        return Blobs.empty();
    }

    bool IsFull() override {
        return Capacity <= UsedSpace;
    }

    void Put(ui64 blobId, TChunkedBuffer&& blob, ui64 /* cookie = 0 */) override {
        if (UsedSpace + blob.Size() > Capacity) {
            ythrow yexception() << "Space limit exceeded";
        }

        auto result = Blobs.emplace(blobId, std::move(blob));
        Y_ABORT_UNLESS(result.second);
        UsedSpace += result.first->second.Size();
    }

    bool Get(ui64 blobId, TBuffer& data, ui64 /* cookie = 0 */) override {
        if (!Blobs.contains(blobId)) {
            ythrow yexception() << "Not found";
        }

        if (GetBlankRequests) {
            --GetBlankRequests;
            return false;
        }

        auto& blob = Blobs[blobId];
        data.Clear();
        const size_t toCopy = blob.Size();
        data.Reserve(toCopy);

        while (!blob.Empty()) {
            auto& buf = blob.Front().Buf;
            data.Append(buf.data(), buf.size());
            blob.Erase(buf.size());
        }

        Y_ABORT_UNLESS(data.size() == toCopy);

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
    THashMap<ui64, TChunkedBuffer> Blobs;
    ui64 UsedSpace = 0;
    ui32 GetBlankRequests = 0;
};

} // namespace NYql::NDq
