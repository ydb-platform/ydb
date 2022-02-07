#pragma once

#include <util/generic/array_ref.h>
#include <util/generic/vector.h>
#include <util/generic/strbuf.h>

#include <array>

namespace NCodecs::NFloatHuff {
    TString Encode(TArrayRef<const float> factors);

    class TDecoder {
    public:
        explicit TDecoder(TStringBuf data);

        TVector<float> DecodeAll(size_t sizeHint = 0);

        // Returns number of decoded floats. May be fewer than requested if the EOS is found.
        size_t Decode(TArrayRef<float> dest);

        // Returns the number of skipped values.
        size_t Skip(size_t count);

        bool HasMore() const;

    private:
        struct TState {
            ui64 Workspace = 0;
            int WorkspaceSize = 0;
            ui64 Position = 0;
            TStringBuf Data;

            ui64 NextBitsUnmasked(int count); // The upper 64 - count bits may be arbitrary
            ui64 PeekBits(int count);
            void SkipBits(int count);
        };

        void FillDecodeBuffer();

        TState State;
        std::array<float, 128> DecodeBuffer;
        // The range of already decompressed numbers inside the DecodeBuffer.
        // Always kept non-empty until the EOS is encountered.
        float* Begin;
        float* End;
        bool HitEos = false;
    };

    TVector<float> Decode(TStringBuf data, size_t sizeHint = 0);
}
