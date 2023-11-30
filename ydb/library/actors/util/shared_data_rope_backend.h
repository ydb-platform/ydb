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

    TMutableContiguousSpan GetDataMut() override {
        if(Buffer.IsShared()) {
            Buffer = TSharedData::Copy(Buffer.data(), Buffer.size());
        }
        return {Buffer.mutable_data(), Buffer.size()};
    }

    TMutableContiguousSpan UnsafeGetDataMut() override {
        return {const_cast<char *>(Buffer.data()), Buffer.size()};
    }

    bool IsPrivate() const override {
        return Buffer.IsPrivate();
    }

    size_t GetOccupiedMemorySize() const override {
        return Buffer.size();
    }
};

} // namespace NActors
