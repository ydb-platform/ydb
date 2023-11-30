#pragma once

#include "coded.h"

#include <util/memory/tempbuf.h>
#include <util/stream/zerocopy.h>
#include <util/stream/mem.h>

namespace NClickHouse {
    class TCompressedInput: public IZeroCopyInput {
    public:
        TCompressedInput(TCodedInputStream* input);
        ~TCompressedInput();

    protected:
        size_t DoNext(const void** ptr, size_t len) override;

        bool Decompress();

    private:
        TCodedInputStream* const Input_;

        TTempBuf Data_;
        TMemoryInput Mem_;
    };

}
