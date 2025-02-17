#include "lzma.h"
#include "private.h"

#include <yt/yt/core/misc/finally.h>

extern "C" {

////////////////////////////////////////////////////////////////////////////////

#include <contrib/libs/lzmasdk/LzmaEnc.h>
#include <contrib/libs/lzmasdk/LzmaDec.h>

////////////////////////////////////////////////////////////////////////////////

} // extern "C"

namespace NYT::NCompression::NDetail {

////////////////////////////////////////////////////////////////////////////////

namespace {

static constexpr auto& Logger = CompressionLogger;

// ISzAlloc is an interface containing alloc/free functions (with its own signatures)
// required by lzma API.
class TSzAlloc
    : public ISzAlloc
{
public:
    TSzAlloc()
    {
        Alloc = &MallocWrap;
        Free = &FreeWrap;
    }

    static void* MallocWrap(const ISzAlloc*, size_t len)
    {
        return malloc(len);
    }

    static void FreeWrap(const ISzAlloc*, void* ptr)
    {
        free(ptr);
    }
};

TSzAlloc Alloc;

class TLzmaSinkWrapper
    : public ISeqOutStream
{
public:
    explicit TLzmaSinkWrapper(TBlob* output)
        : Output_(output)
    {
        Write = &TLzmaSinkWrapper::WriteDataProc;
    }

    size_t WriteData(const void* buffer, size_t size)
    {
        Output_->Append(buffer, size);
        return size;
    }

    static size_t WriteDataProc(const ISeqOutStream* lzmaWriteWrapper, const void* buffer, size_t size)
    {
        TLzmaSinkWrapper* pThis = const_cast<TLzmaSinkWrapper*>(static_cast<const TLzmaSinkWrapper*>(lzmaWriteWrapper));
        return pThis->WriteData(buffer, size);
    }

private:
    TBlob* const Output_;
};

class TLzmaSourceWrapper
    : public ISeqInStream
{
public:
    explicit TLzmaSourceWrapper(TSource* source)
        : Source_(source)
    {
        Read = ReadDataProc;
    }

    SRes ReadData(void* buffer, size_t* size)
    {
        size_t peekedSize;
        const void* peekedData = Source_->Peek(&peekedSize);
        peekedSize = std::min(*size, peekedSize);
        peekedSize = std::min(peekedSize, Source_->Available());

        memcpy(buffer, peekedData, peekedSize);

        Source_->Skip(peekedSize);
        *size = peekedSize;
        return SZ_OK;
    }

    static SRes ReadDataProc(const ISeqInStream* lzmaReadWrapper, void* buffer, size_t* size)
    {
        TLzmaSourceWrapper* pThis = const_cast<TLzmaSourceWrapper*>(static_cast<const TLzmaSourceWrapper*>(lzmaReadWrapper));
        return pThis->ReadData(buffer, size);
    }

private:
    TSource* const Source_;
};

} // namespace

void LzmaCompress(int level, TSource* source, TBlob* output)
{
    YT_VERIFY(0 <= level && level <= 9);

    TLzmaSourceWrapper reader(source);
    TLzmaSinkWrapper writer(output);

    auto handle = LzmaEnc_Create(&Alloc);
    YT_VERIFY(handle);

    auto finallyGuard = Finally([&] {
        LzmaEnc_Destroy(handle, &Alloc, &Alloc);
    });

    auto checkError = [] (SRes result) {
        YT_LOG_FATAL_IF(result != SZ_OK, "Lzma compression failed (Error: %v)",
            result);
    };

    {
        // Set properties.
        CLzmaEncProps props;
        LzmaEncProps_Init(&props);
        props.level = level;
        props.writeEndMark = 1;
        checkError(LzmaEnc_SetProps(handle, &props));
    }

    {
        // Write properties.
        Byte propsBuffer[LZMA_PROPS_SIZE];
        size_t propsBufferSize = LZMA_PROPS_SIZE;
        checkError(LzmaEnc_WriteProperties(handle, propsBuffer, &propsBufferSize));
        writer.WriteData(propsBuffer, sizeof(propsBuffer));
    }

    // Compress data.
    checkError(LzmaEnc_Encode(handle, &writer, &reader, nullptr, &Alloc, &Alloc));
}

void LzmaDecompress(TSource* source, TBlob* output)
{
    Byte propsBuffer[LZMA_PROPS_SIZE];
    ReadRef(*source, TMutableRef(reinterpret_cast<char*>(propsBuffer), sizeof(propsBuffer)));

    CLzmaDec handle;
    LzmaDec_Construct(&handle);

    {
        auto result = LzmaDec_Allocate(&handle, propsBuffer, LZMA_PROPS_SIZE, &Alloc);
        if (result != SZ_OK) {
            THROW_ERROR_EXCEPTION("Lzma decompression failed: LzmaDec_Allocate returned an error")
                << TErrorAttribute("error", result);
        }
    }

    auto finallyGuard = Finally([&] {
        LzmaDec_Free(&handle, &Alloc);
    });

    LzmaDec_Init(&handle);

    auto status = LZMA_STATUS_NOT_FINISHED;
    while (source->Available() > 0) {
        size_t sourceDataSize;
        const Byte* sourceData = reinterpret_cast<const Byte*>(source->Peek(&sourceDataSize));

        sourceDataSize = std::min(sourceDataSize, source->Available());

        size_t oldDicPos = handle.dicPos;
        size_t bufferSize = sourceDataSize;

        {
            auto result = LzmaDec_DecodeToDic(
                &handle,
                handle.dicBufSize,
                sourceData,
                &bufferSize, // It's input buffer size before call, read byte count afterwards.
                LZMA_FINISH_ANY,
                &status);
            if (result != SZ_OK) {
                THROW_ERROR_EXCEPTION("Lzma decompression failed: LzmaDec_DecodeToDic returned an error")
                    << TErrorAttribute("error", result);
            }
        }

        output->Append(handle.dic + oldDicPos, handle.dicPos - oldDicPos);

        sourceData += bufferSize;
        sourceDataSize -= bufferSize;

        // Strange lzma api requires us to update this index by hand.
        if (handle.dicPos == handle.dicBufSize) {
            handle.dicPos = 0;
        }

        source->Skip(bufferSize);
    }

    if (status != LZMA_STATUS_FINISHED_WITH_MARK) {
        THROW_ERROR_EXCEPTION("Lzma decompression failed: unexpected final status")
            << TErrorAttribute("status", static_cast<int>(status));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression::NDetail
