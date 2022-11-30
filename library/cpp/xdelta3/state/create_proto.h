#pragma once

#include "state.h"

#include <library/cpp/xdelta3/proto/state_header.pb.h>

#include <functional>

namespace NXdeltaAggregateColumn {

    // functor used for testing - change header fields to inject errors
    using TChangeHeader = std::function<void(TStateHeader&)>;

    TBuffer EncodeErrorProtoAsBuffer(TStateHeader::EErrorCode error, const TChangeHeader& changeHeader = {});
    TBuffer EncodeBaseProtoAsBuffer(const ui8* base, size_t baseSize, const TChangeHeader& changeHeader = {});
    TBuffer EncodePatchProtoAsBuffer(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const TChangeHeader& changeHeader = {});
    TBuffer EncodePatchProtoAsBuffer(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const ui8* patch, size_t patchSize, const TChangeHeader& changeHeader = {});
    TBuffer EncodeProtoAsBuffer(const TStateHeader& header, const ui8* data, size_t size);

    TString EncodeErrorProtoAsString(TStateHeader::EErrorCode error, const TChangeHeader& changeHeader = {});
    TString EncodeBaseProtoAsString(const ui8* base, size_t baseSize, const TChangeHeader& changeHeader = {});
    TString EncodePatchProtoAsString(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const TChangeHeader& changeHeader = {});
    TString EncodePatchProtoAsString(const ui8* base, size_t baseSize, const ui8* state, size_t stateSize, const ui8* patch, size_t patchSize, const TChangeHeader& changeHeader = {});
    TString EncodeProtoAsString(const TStateHeader& header, const ui8* data, size_t size);

}
