#pragma once

#include <library/cpp/actors/util/rope.h>

#include "shared_data.h"

namespace NKikimr {

class TRopeSharedDataBackend : public IRopeChunkBackend {
    TSharedData Buffer;

public:
    TRopeSharedDataBackend(TSharedData buffer)
        : Buffer(std::move(buffer))
    {}

    TData GetData() const override {
        return {Buffer.data(), Buffer.size()};
    }

    size_t GetCapacity() const override {
        return Buffer.size();
    }
};

} // namespace NKikimr
