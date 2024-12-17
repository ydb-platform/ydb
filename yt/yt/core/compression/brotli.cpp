#include "brotli.h"
#include "private.h"

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/finally.h>

#include <library/cpp/streams/brotli/brotli.h>

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CompressionLogger;

////////////////////////////////////////////////////////////////////////////////

void BrotliCompress(int level, TSource* source, TBlob* output)
{
    ui64 totalInputSize = source->Available();
    output->Resize(sizeof(totalInputSize), /*initializeStorage*/ false);

    // Write input size that will be used during decompression.
    TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
    WritePod(memoryOutput, totalInputSize);

    TBlobSink sink(output);
    try {
        TBrotliCompress compress(&sink, level);
        while (source->Available() > 0) {
            size_t read;
            const char* ptr = source->Peek(&read);
            if (read > 0) {
                compress.Write(ptr, read);
                source->Skip(read);
            }
        }
    } catch (const std::exception& ex) {
        YT_LOG_FATAL(ex, "Brotli compression failed");
    }
}

void BrotliDecompress(TSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(*source, outputSize);

    output->Resize(outputSize, /*initializeStorage*/ false);

    TBrotliDecompress decompress(source);
    ui64 remainingSize = outputSize;
    while (remainingSize > 0) {
        ui64 offset = outputSize - remainingSize;
        ui64 read = decompress.Read(output->Begin() + offset, remainingSize);
        if (read == 0) {
            break;
        }
        remainingSize -= read;
    }

    if (remainingSize != 0) {
        THROW_ERROR_EXCEPTION("Brotli decompression failed: input stream is not fully consumed")
            << TErrorAttribute("remaining_size", remainingSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail

