#pragma once

#include <ydb/library/yql/dq/runtime/dq_channel_storage.h>

#include <util/generic/hash.h>

#include <mutex>
#include <optional>

namespace NYql::NDq {


class TMockChannelStorage : public IDqChannelStorage {
public:
    TMockChannelStorage(ui64 capacity)
        : Capacity(capacity) {}

    bool IsEmpty() override {
        std::lock_guard lock(Mutex);
        return Blobs.empty();
    }

    bool IsFull() override {
        std::lock_guard lock(Mutex);
        return Full || Capacity <= UsedSpace;
    }

    void Put(ui64 blobId, TChunkedBuffer&& blob, ui64 /* cookie = 0 */) override {
        std::lock_guard lock(Mutex);
        if (UsedSpace + blob.Size() > Capacity) {
            ythrow yexception() << "Space limit exceeded";
        }

        auto result = Blobs.emplace(blobId, std::move(blob));
        Y_ABORT_UNLESS(result.second);
        UsedSpace += result.first->second.Size();
        PutCount++;
        LastPutBlobId = blobId;
    }

    bool Get(ui64 blobId, TBuffer& data, ui64 /* cookie = 0 */) override {
        std::lock_guard lock(Mutex);
        if (!Blobs.contains(blobId)) {
            ythrow yexception() << "Not found";
        }

        if (GetBlankRequests) {
            --GetBlankRequests;
            return false;
        }
        if (StuckBlobId && *StuckBlobId == blobId) {
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
        GetCount++;

        return true;
    }

    void SetWakeUpCallback(TWakeUpCallback&& wakeUpCallback) override {
        std::lock_guard lock(Mutex);
        WakeUpCallback = std::move(wakeUpCallback);
    }

public:
    // the number of Get() calls to answer "not ready yet" with, from the calling thread; the test then
    // drives the deferred load with WakeUp() as the real storage actor would
    void SetBlankGetRequests(ui32 count) {
        std::lock_guard lock(Mutex);
        GetBlankRequests = count;
    }

    // this blob is "not ready yet" for every Get() until released, whatever the others do
    void SetStuckBlob(std::optional<ui64> blobId) {
        std::lock_guard lock(Mutex);
        StuckBlobId = blobId;
    }

    ui64 GetLastPutBlobId() {
        std::lock_guard lock(Mutex);
        return LastPutBlobId;
    }

    // forces IsFull() regardless of the capacity, to stage the HardLimit / SoftLimit transitions
    void SetFull(bool full) {
        std::lock_guard lock(Mutex);
        Full = full;
    }

    void WakeUp() {
        TWakeUpCallback callback;
        {
            std::lock_guard lock(Mutex);
            callback = WakeUpCallback;
        }
        if (callback) {
            callback();
        }
    }

    ui64 GetPutCount() {
        std::lock_guard lock(Mutex);
        return PutCount;
    }

    ui64 GetGetCount() {
        std::lock_guard lock(Mutex);
        return GetCount;
    }

private:
    // the channel service calls Put/Get from the producer and consumer threads and the test polls
    // the state from its own, so every access is under the mutex
    std::mutex Mutex;
    const ui64 Capacity;
    THashMap<ui64, TChunkedBuffer> Blobs;
    ui64 UsedSpace = 0;
    ui32 GetBlankRequests = 0;
    std::optional<ui64> StuckBlobId;
    ui64 LastPutBlobId = 0;
    bool Full = false;
    ui64 PutCount = 0;
    ui64 GetCount = 0;
    TWakeUpCallback WakeUpCallback;
};

} // namespace NYql::NDq
