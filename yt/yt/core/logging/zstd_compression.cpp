#include "zstd_compression.h"

#include "compression.h"

#include <yt/yt/core/misc/finally.h>

#include <yt/yt/core/compression/zstd.h>

#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NLogging {

using namespace NCompression::NDetail;

////////////////////////////////////////////////////////////////////////////////

class TZstdLogCompressionCodec
    : public ILogCompressionCodec
{
public:
    explicit TZstdLogCompressionCodec(int compressionLevel)
        : CompressionLevel_(compressionLevel)
    { }

    i64 GetMaxBlockSize() const override
    {
        return MaxZstdFrameUncompressedLength;
    }

    void Compress(const TBuffer& input, TBuffer& output) override
    {
        auto context = ZSTD_createCCtx();
        auto contextGuard = Finally([&] {
            ZSTD_freeCCtx(context);
        });

        auto frameLength = ZSTD_COMPRESSBOUND(std::min<i64>(MaxZstdFrameUncompressedLength, input.Size()));

        output.Reserve(output.Size() + frameLength + ZstdSyncTagSize);
        size_t size = ZSTD_compressCCtx(
            context,
            output.Data() + output.Size(),
            frameLength,
            input.Data(),
            input.Size(),
            CompressionLevel_);

        if (ZSTD_isError(size)) {
            THROW_ERROR_EXCEPTION("ZSTD_compressCCtx() failed")
                << TErrorAttribute("zstd_error", ZSTD_getErrorName(size));
        }
        output.Advance(size);
    }

    void AddSyncTag(i64 offset, TBuffer& output) override
    {
        output.Append(ZstdSyncTagPrefix.data(), ZstdSyncTagPrefix.size());
        output.Append(reinterpret_cast<const char*>(&offset), sizeof(offset));
    }

    void Repair(TFile* file, i64& outputPosition) override
    {
        constexpr auto ScanOverlap = ZstdSyncTagSize - 1;
        constexpr auto MaxZstdFrameLength = ZSTD_COMPRESSBOUND(MaxZstdFrameUncompressedLength);
        constexpr auto TailScanLength = MaxZstdFrameLength + 2 * ZstdSyncTagSize;

        i64 fileSize = file->GetLength();
        i64 bufSize = fileSize;
        i64 pos = Max<i64>(bufSize - TailScanLength, 0);
        bufSize -= pos;

        TBuffer buffer;

        outputPosition = 0;
        while (bufSize >= ZstdSyncTagSize) {
            buffer.Resize(0);
            buffer.Reserve(bufSize);

            size_t sz = file->Pread(buffer.Data(), bufSize, pos);
            buffer.Resize(sz);

            if (auto offset = FindLastZstdSyncTagOffset(TRef(buffer.Data(), buffer.Size()), pos)) {
                outputPosition = *offset + ZstdSyncTagSize;
                break;
            }

            i64 newPos = Max<i64>(pos - TailScanLength, 0);
            bufSize = Max<i64>(pos + ScanOverlap - newPos, 0);
            pos = newPos;
        }
        file->Resize(outputPosition);
    }

private:
    int CompressionLevel_;
};

DECLARE_REFCOUNTED_TYPE(TZstdLogCompressionCodec)
DEFINE_REFCOUNTED_TYPE(TZstdLogCompressionCodec)

ILogCompressionCodecPtr CreateZstdCompressionCodec(int compressionLevel)
{
    return New<TZstdLogCompressionCodec>(compressionLevel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
