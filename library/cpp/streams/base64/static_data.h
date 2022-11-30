#pragma once

#include "base64stream.h"

#include <util/memory/blob.h>
#include <util/stream/zlib.h>
#include <util/generic/ptr.h>

class TCompressedStaticData {
public:
    using TDataFunction = const char* (*)(size_t);
    using TData = TBlob;

    class TDataInputStream: public IInputStream {
    public:
        TDataInputStream(TDataFunction function)
            : Compressed(function)
            , Decompressed(&Compressed)
        {
        }

    private:
        size_t DoRead(void* buf, size_t len) override {
            return Decompressed.Read(buf, len);
        }

    private:
        TStringDataInputStream Compressed;
        TZLibDecompress Decompressed;
    };

    TCompressedStaticData(TDataFunction function) {
        TDataInputStream inp(function);
        Data = TBlob::FromStream(inp);
    }

    const TData& GetData() const {
        return Data;
    }

private:
    TData Data;
};
