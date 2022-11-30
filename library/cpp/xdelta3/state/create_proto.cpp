#include "create_proto.h"

#include <library/cpp/xdelta3/state/data_ptr.h>
#include <library/cpp/xdelta3/state/hash.h>
#include <library/cpp/xdelta3/xdelta_codec/codec.h>

namespace NXdeltaAggregateColumn {
    bool EncodeHeaderTo(const TStateHeader& header, ui8* data, size_t size, size_t& resultSize);
}

namespace {
    using namespace NXdeltaAggregateColumn;

    template<typename TResult>
    TResult EncodeProto(const TStateHeader& header, const ui8* data, size_t size)
    {
        using namespace NProtoBuf::io;

        TResult result;
        ui8 totalHeaderSize = SizeOfHeader(header);
        ui8* ptr = nullptr;
        if constexpr (std::is_same_v<TResult, TString>) {
            result.ReserveAndResize(totalHeaderSize + size);
            ptr = reinterpret_cast<ui8*>(&result[0]);
        } else {
            result.Resize(totalHeaderSize + size);
            ptr = reinterpret_cast<ui8*>(result.Data());
        }
        size_t resultSize = 0;

        if (EncodeHeaderTo(header, ptr, result.Size(), resultSize)) {
            if (data && size) {
                memcpy(ptr + totalHeaderSize, data, size);
            }
            return result;
        }
        return {};
    }

    template<typename TResult>
    TResult EncodeErrorProto(TStateHeader::EErrorCode error, const TChangeHeader& changeHeader = {})
    {
        TStateHeader header;
        header.set_error_code(error);

        if (changeHeader) {
            changeHeader(header);
        }

        return EncodeProto<TResult>(header, nullptr, 0);
    }

    template<typename TResult>
    TResult EncodeBaseProto(const ui8* base, size_t baseSize, const TChangeHeader& changeHeader = {})
    {
        TStateHeader header;
        header.set_type(TStateHeader::BASE);
        header.set_data_size(baseSize);

        if (changeHeader) {
            changeHeader(header);
        }

        return EncodeProto<TResult>(header, base, baseSize);
    }

    template<typename TResult>
    TResult EncodePatchProto(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const ui8* patch, size_t patchSize, const TChangeHeader& changeHeader  = {}) 
    {
        TStateHeader header;
        header.set_type(TStateHeader::PATCH);
        header.set_data_size(patchSize);
        header.set_state_hash(CalcHash(state, stateSize));
        header.set_state_size(stateSize);
        header.set_base_hash(CalcHash(base, baseSize));

        if (changeHeader) {
            changeHeader(header);
        }

        return EncodeProto<TResult>(header, patch, patchSize);
    }

    template<typename TResult>
    TResult EncodePatchProto(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const TChangeHeader& changeHeader  = {})
    {
        size_t patchSize = 0;
        auto patch = TDataPtr(ComputePatch(nullptr, base, baseSize, state, stateSize, &patchSize));

        return EncodePatchProto<TResult>(base, baseSize, state, stateSize, patch.get(), patchSize, changeHeader);
    }
}

namespace NXdeltaAggregateColumn {
    TBuffer EncodeErrorProtoAsBuffer(TStateHeader::EErrorCode error, const TChangeHeader& changeHeader) {
        return EncodeErrorProto<TBuffer>(error, changeHeader);
    }

    TBuffer EncodeBaseProtoAsBuffer(const ui8* base, size_t baseSize, const TChangeHeader& changeHeader) {
        return EncodeBaseProto<TBuffer>(base, baseSize, changeHeader);
    }

    TBuffer EncodePatchProtoAsBuffer(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const TChangeHeader& changeHeader) {
        return EncodePatchProto<TBuffer>(base, baseSize, state, stateSize, changeHeader);
    }

    TBuffer EncodePatchProtoAsBuffer(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const ui8* patch, size_t patchSize, const TChangeHeader& changeHeader) {
        return EncodePatchProto<TBuffer>(base, baseSize, state, stateSize, patch, patchSize, changeHeader);
    }

    TBuffer EncodeProtoAsBuffer(const TStateHeader& header, const ui8* data, size_t size) {
        return EncodeProto<TBuffer>(header, data, size);
    }

    TString EncodeErrorProtoAsString(TStateHeader::EErrorCode error, const TChangeHeader& changeHeader) {
        return EncodeErrorProto<TString>(error, changeHeader);
    }

    TString EncodeBaseProtoAsString(const ui8* base, size_t baseSize, const TChangeHeader& changeHeader) {
        return EncodeBaseProto<TString>(base, baseSize, changeHeader);
    }

    TString EncodePatchProtoAsString(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const TChangeHeader& changeHeader) {
        return EncodePatchProto<TString>(base, baseSize, state, stateSize, changeHeader);
    }

    TString EncodePatchProtoAsString(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const ui8* patch, size_t patchSize, const TChangeHeader& changeHeader) {
        return EncodePatchProto<TString>(base, baseSize, state, stateSize, patch, patchSize, changeHeader);
    }

    TString EncodeProtoAsString(const TStateHeader& header, const ui8* data, size_t size) {
        return EncodeProto<TString>(header, data, size);
    }
}
