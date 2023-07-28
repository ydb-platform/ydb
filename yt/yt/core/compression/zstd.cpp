#include "zstd.h"
#include "private.h"

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/finally.h>

#define ZSTD_STATIC_LINKING_ONLY
#include <contrib/libs/zstd/include/zstd.h>

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CompressionLogger;

////////////////////////////////////////////////////////////////////////////////

struct TZstdCompressBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

void ZstdCompress(int level, TSource* source, TBlob* output)
{
    ui64 totalInputSize = source->Available();
    output->Resize(ZSTD_compressBound(totalInputSize) + sizeof(totalInputSize), /*initializeStorage*/ false);
    size_t curOutputPos = 0;

    // Write input size that will be used during decompression.
    {
        TMemoryOutput memoryOutput(output->Begin(), sizeof(totalInputSize));
        WritePod(memoryOutput, totalInputSize);
        curOutputPos += sizeof(totalInputSize);
    }

    auto checkError = [] (size_t result) {
        YT_LOG_FATAL_IF(ZSTD_isError(result), "Zstd compression failed (Error: %v)",
            ZSTD_getErrorName(result));
    };

    auto context = ZSTD_createCCtx();
    auto contextGuard = Finally([&] () {
       ZSTD_freeCCtx(context);
    });

    {
        auto result = ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel, level);
        checkError(result);
    }

    struct CompressResult
    {
        size_t Result;
        size_t ProcessedBytes;
    };

    auto compressToOutput = [&] (const char* buffer, size_t size, bool isLastBlock) -> CompressResult {
        ZSTD_EndDirective mode = isLastBlock ? ZSTD_e_end : ZSTD_e_continue;

        ZSTD_inBuffer zstdInput = {buffer, size, 0};
        ZSTD_outBuffer zstdOutput = {output->Begin(), output->Size(), curOutputPos};

        size_t result = ZSTD_compressStream2(context, &zstdOutput, &zstdInput, mode);
        checkError(result);
        curOutputPos = zstdOutput.pos;

        return CompressResult{
            .Result = result,
            .ProcessedBytes = zstdInput.pos,
        };
    };

    auto recommendedInputSize = ZSTD_CStreamInSize();

    auto block = TBlob(GetRefCountedTypeCookie<TZstdCompressBufferTag>());
    size_t blockSize = 0;

    auto compressDataFromBlock = [&] (bool isLastBlock) {
        size_t remainingSize = blockSize;
        while (remainingSize > 0) {
            auto compressResult = compressToOutput(
                block.Begin() + (blockSize - remainingSize),
                remainingSize,
                isLastBlock);
            remainingSize -= compressResult.ProcessedBytes;
            if (isLastBlock ? compressResult.Result == 0 : remainingSize == 0) {
                break;
            }
        }
        blockSize = 0;
    };

    while (source->Available() > 0) {
        size_t inputSize;
        const char* inputPtr = source->Peek(&inputSize);

        bool shouldFlushBlock = false;
        if (inputSize < recommendedInputSize) {
            if (block.IsEmpty()) {
                block.Resize(recommendedInputSize, /*initializeStorage*/ false);
            }
            if (blockSize + inputSize <= block.Size()) {
                // Append to block.
                memcpy(block.Begin() + blockSize, inputPtr, inputSize);
                blockSize += inputSize;
                source->Skip(inputSize);
                continue;
            } else {
                shouldFlushBlock = true;
            }
        } else {
            shouldFlushBlock = blockSize > 0;
        }

        if (shouldFlushBlock) {
            compressDataFromBlock(/*isLastBlock*/ false);
        }

        size_t remainingSize = inputSize;

        bool isLastBlock = source->Available() == remainingSize;
        while (true) {
            auto compressResult = compressToOutput(
                inputPtr + (inputSize - remainingSize),
                remainingSize,
                isLastBlock);
            source->Skip(compressResult.ProcessedBytes);
            remainingSize -= compressResult.ProcessedBytes;
            if (isLastBlock ? compressResult.Result == 0 : remainingSize == 0) {
                break;
            }
        }
    }

    if (blockSize > 0) {
        compressDataFromBlock(/*isLastBlock*/ true);
    }

    output->Resize(curOutputPos);
}

void ZstdDecompress(TSource* source, TBlob* output)
{
    ui64 outputSize;
    ReadPod(*source, outputSize);

    output->Resize(outputSize, /*initializeStorage*/ false);
    void* outputPtr = output->Begin();

    size_t inputSize;
    const void* inputPtr = source->Peek(&inputSize);

    // TODO(babenko): something weird is going on here.
    TBlob input;
    if (auto available = source->Available(); available > inputSize) {
        input.Resize(available, /*initializeStorage*/ false);
        ReadRef(*source, TMutableRef(input.Begin(), available));
        inputPtr = input.Begin();
        inputSize = input.Size();
    }

    size_t decompressedSize = ZSTD_decompress(outputPtr, outputSize, inputPtr, inputSize);
    if (ZSTD_isError(decompressedSize)) {
        THROW_ERROR_EXCEPTION("Zstd decompression failed: ZSTD_decompress returned an error")
            << TErrorAttribute("error", ZSTD_getErrorName(decompressedSize));
    }
    if (decompressedSize != outputSize) {
        THROW_ERROR_EXCEPTION("Zstd decompression failed: output size mismatch")
            << TErrorAttribute("expected_size", outputSize)
            << TErrorAttribute("actual_size", decompressedSize);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail

