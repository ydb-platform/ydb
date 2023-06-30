#include "zlib.h"

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/yt/assert/assert.h>

#include <zlib.h>

#include <array>
#include <iostream>

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

static constexpr size_t MaxZlibUInt = std::numeric_limits<uInt>::max();

////////////////////////////////////////////////////////////////////////////////

void ZlibCompress(int level, TSource* source, TBlob* output)
{
    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int ret = deflateInit(&stream, level);
    YT_VERIFY(ret == Z_OK);

    auto finallyGuard = Finally([&] {
        deflateEnd(&stream);
    });

    size_t totalUncompressedSize = source->Available();
    size_t totalCompressedSize = deflateBound(&stream, totalUncompressedSize);

    // Write out header.
    output->Reserve(sizeof(ui64) + totalCompressedSize);
    output->Resize(sizeof(ui64), /*initializeStorage*/ false);
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(ui64));
        WritePod(memoryOutput, static_cast<ui64>(totalUncompressedSize)); // Force serialization type.
    }

    // Write out body.
    do {
        size_t inputAvailable;
        const char* inputNext = source->Peek(&inputAvailable);
        inputAvailable = std::min(inputAvailable, source->Available());
        inputAvailable = std::min(inputAvailable, MaxZlibUInt);

        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputNext));
        stream.avail_in = static_cast<uInt>(inputAvailable);

        int flush = (stream.avail_in == source->Available()) ? Z_FINISH : Z_NO_FLUSH;

        do {
            size_t outputAvailable = output->Capacity() - output->Size();
            if (outputAvailable == 0) {
                // XXX(sandello): Somehow we have missed our buffer estimate.
                // Reallocate it to provide enough capacity.
                size_t sourceAvailable = source->Available() - inputAvailable + stream.avail_in;
                outputAvailable = deflateBound(&stream, sourceAvailable);
                output->Reserve(output->Size() + outputAvailable);
            }
            outputAvailable = std::min(outputAvailable, MaxZlibUInt);

            stream.next_out = reinterpret_cast<Bytef*>(output->End());
            stream.avail_out = static_cast<uInt>(outputAvailable);

            ret = deflate(&stream, flush);
            // We should not throw exception here since caller does not expect such behavior.
            YT_VERIFY(ret == Z_OK || ret == Z_STREAM_END);

            output->Resize(output->Size() + outputAvailable - stream.avail_out, /*initializeStorage*/ false);
        } while (stream.avail_out == 0);

        // Entire input was consumed.
        YT_VERIFY(stream.avail_in == 0);
        source->Skip(inputAvailable - stream.avail_in);
    } while (source->Available() > 0);

    YT_VERIFY(ret == Z_STREAM_END);
}

void ZlibDecompress(TSource* source, TBlob* output)
{
    if (source->Available() == 0) {
        return;
    }

    // Read header.
    ui64 totalUncompressedSize;
    ReadPod(*source, totalUncompressedSize);

    // We add one additional byte to process correctly last inflate.
    output->Reserve(totalUncompressedSize + 1);

    z_stream stream{};
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    YT_VERIFY(inflateInit(&stream) == Z_OK);

    auto finallyGuard = Finally([&] {
        inflateEnd(&stream);
    });

    // Read body.
    int result;
    do {
        size_t inputAvailable;
        const char* inputNext = source->Peek(&inputAvailable);
        inputAvailable = std::min(inputAvailable, source->Available());
        inputAvailable = std::min(inputAvailable, MaxZlibUInt);

        stream.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(inputNext));
        stream.avail_in = static_cast<uInt>(inputAvailable);

        int flush = (stream.avail_in == source->Available()) ? Z_FINISH : Z_NO_FLUSH;

        size_t outputAvailable = output->Capacity() - output->Size();
        outputAvailable = std::min(outputAvailable, MaxZlibUInt);

        stream.next_out = reinterpret_cast<Bytef*>(output->End());
        stream.avail_out = static_cast<uInt>(outputAvailable);

        result = inflate(&stream, flush);
        if (result != Z_OK && result != Z_STREAM_END) {
            THROW_ERROR_EXCEPTION("Zlib compression failed: inflate returned an error")
                << TErrorAttribute("error", result);
        }

        source->Skip(inputAvailable - stream.avail_in);

        output->Resize(output->Size() + outputAvailable - stream.avail_out, /*initializeStorage*/ false);
    } while (result != Z_STREAM_END);

    if (source->Available() != 0) {
        THROW_ERROR_EXCEPTION("Zlib compression failed: input stream is not fully consumed")
            << TErrorAttribute("remaining_size", source->Available());
    }
    if (output->Size() != totalUncompressedSize) {
        THROW_ERROR_EXCEPTION("Zlib decompression failed: output size mismatch")
            << TErrorAttribute("expected_size", totalUncompressedSize)
            << TErrorAttribute("actual_size", output->Size());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail
