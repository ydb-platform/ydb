#pragma once

#include <library/cpp/xdelta3/xdelta_codec/codec.h>

#include <memory>

namespace NXdeltaAggregateColumn {
    struct TDeleter {
        TDeleter() = default;
        explicit TDeleter(XDeltaContext* context);

        void operator()(ui8* p) const;

        XDeltaContext* Context = nullptr;
    };
    using TDataPtr = std::unique_ptr<ui8, TDeleter>;
}
