#ifndef KIKIMR_DISABLE_S3_OPS

#include "export_s3_buffer_raw.h"

#include <contrib/libs/zstd/include/zstd.h>

namespace {

    struct DestroyZCtx {
        static void Destroy(::ZSTD_CCtx* p) noexcept {
            ZSTD_freeCCtx(p);
        }
    };

} // anonymous

namespace NKikimr {
namespace NDataShard {

class TS3BufferZstd: public TS3BufferRaw {
    enum ECompressionResult {
        CONTINUE,
        DONE,
        ERROR,
    };

    ECompressionResult Compress(ZSTD_inBuffer* input, ZSTD_EndDirective endOp) {
        auto output = ZSTD_outBuffer{Buffer.Data(), Buffer.Capacity(), Buffer.Size()};
        auto res = ZSTD_compressStream2(Context.Get(), &output, input, endOp);

        if (ZSTD_isError(res)) {
            ErrorCode = res;
            return ERROR;
        }

        if (res > 0) {
            Buffer.Reserve(output.pos + res);
        }

        Buffer.Proceed(output.pos);
        return res ? CONTINUE : DONE;
    }

    void Reset() {
        ZSTD_CCtx_reset(Context.Get(), ZSTD_reset_session_only);
        ZSTD_CCtx_refCDict(Context.Get(), NULL);
        ZSTD_CCtx_setParameter(Context.Get(), ZSTD_c_compressionLevel, CompressionLevel);
    }

public:
    explicit TS3BufferZstd(int compressionLevel,
            const IExport::TTableColumns& columns, ui64 maxRows, ui64 maxBytes, ui64 minBytes)
        : TS3BufferRaw(columns, maxRows, maxBytes)
        , CompressionLevel(compressionLevel)
        , MinBytes(minBytes)
        , Context(ZSTD_createCCtx())
        , ErrorCode(0)
        , BytesRaw(0)
    {
        Reset();
    }

    bool Collect(const NTable::IScan::TRow& row) override {
        BufferRaw.clear();
        TStringOutput out(BufferRaw);
        if (!TS3BufferRaw::Collect(row, out)) {
            return false;
        }

        BytesRaw += BufferRaw.size();

        auto input = ZSTD_inBuffer{BufferRaw.data(), BufferRaw.size(), 0};
        while (input.pos < input.size) {
            if (ERROR == Compress(&input, ZSTD_e_continue)) {
                return false;
            }
        }

        return true;
    }

    bool IsFilled() const override {
        if (Buffer.Size() < MinBytes) {
            return false;
        }

        return Rows >= GetRowsLimit() || BytesRaw >= GetBytesLimit();
    }

    TString GetError() const override {
        return ZSTD_getErrorName(ErrorCode);
    }
    
protected:
    TMaybe<TBuffer> Flush(bool prepare) override {
        if (prepare && BytesRaw) {
            ECompressionResult res;
            auto input = ZSTD_inBuffer{NULL, 0, 0};

            do {
                if (res = Compress(&input, ZSTD_e_end); res == ERROR) {
                    return Nothing();
                }
            } while (res != DONE);
        }

        Reset();

        BytesRaw = 0;
        return TS3BufferRaw::Flush(prepare);
    }

private:
    const int CompressionLevel;
    const ui64 MinBytes;

    THolder<::ZSTD_CCtx, DestroyZCtx> Context;
    size_t ErrorCode;
    ui64 BytesRaw;
    TString BufferRaw;

}; // TS3BufferZstd

NExportScan::IBuffer* CreateS3ExportBufferZstd(int compressionLevel,
        const IExport::TTableColumns& columns, ui64 maxRows, ui64 maxBytes, ui64 minBytes)
{
    return new TS3BufferZstd(compressionLevel, columns, maxRows, maxBytes, minBytes);
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_S3_OPS
