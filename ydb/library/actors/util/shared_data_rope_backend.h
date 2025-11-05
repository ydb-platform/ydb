#pragma once

#include <ydb/library/actors/util/rc_buf.h>

#include "shared_data.h"

namespace NActors {

class TRopeSharedDataBackend : public IContiguousChunk {
    TSharedData Buffer;

public:
    TRopeSharedDataBackend(TSharedData buffer)
        : Buffer(std::move(buffer))
    {}

    TContiguousSpan GetData() const override {
        return {Buffer.data(), Buffer.size()};
    }

    TMutableContiguousSpan UnsafeGetDataMut() override {
        // Actualy should newer be true, but acciording to the API this class can share TSharedData with some external buffer
        if (Buffer.IsShared()) {
            Buffer = TSharedData::Copy(Buffer.data(), Buffer.size());
        }
        return {Buffer.mutable_data(), Buffer.size()};
    }

    bool IsPrivate() const override {
        return RefCount() == 1 && Buffer.IsPrivate();
    }

    size_t GetOccupiedMemorySize() const override {
        return Buffer.size();
    }

    IContiguousChunk::TPtr Clone() override {
        auto backend = MakeIntrusive<TRopeSharedDataBackend>(Buffer);
        backend->Buffer.Detach();
        return backend;
    }
};

} // namespace NActors
