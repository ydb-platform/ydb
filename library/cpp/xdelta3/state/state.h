#pragma once

#include <library/cpp/xdelta3/proto/state_header.pb.h>

#include <google/protobuf/arena.h>

namespace NXdeltaAggregateColumn {

    constexpr auto ArenaMaxSize = 65525;

    class TState {
    public:
        TState(NProtoBuf::Arena& arena, const ui8* data, size_t size);

        const NXdeltaAggregateColumn::TStateHeader& Header() const
        {
            return *HeaderPtr;
        }

        const ui8* PayloadData() const
        {
            return Data;
        }

        size_t PayloadSize() const;
        TStateHeader::EErrorCode Error() const;
        TStateHeader::EType Type() const;

        ui32 CalcHash() const;

    private:
        const ui8* Data = nullptr;
        TStateHeader* HeaderPtr = nullptr;
    };

    size_t SizeOfHeader(const TStateHeader& header);
}
