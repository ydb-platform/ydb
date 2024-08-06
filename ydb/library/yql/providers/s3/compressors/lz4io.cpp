#include "lz4io.h"

#include <util/generic/scope.h>
#include <util/generic/size_literals.h>

#include <contrib/libs/lz4/lz4.h>
#include <contrib/libs/lz4/lz4hc.h>

#include <ydb/library/yql/utils/yql_panic.h>
#include "output_queue_impl.h"

namespace NYql {

namespace NLz4 {

namespace {

constexpr ui32 MagicNumberSize  = 4U;
constexpr ui32 Lz4ioMagicNumber =  0x184D2204U;
constexpr ui32 LegacyMagicNumber = 0x184C2102U;

constexpr size_t LegacyBlockSize = 8_MB;
constexpr size_t FrameMaxBlockSize = 4_MB;

void WriteLE32(void* p, ui32 value32)
{
    const auto dstPtr = static_cast<unsigned char*>(p);
    dstPtr[0] = (unsigned char)value32;
    dstPtr[1] = (unsigned char)(value32 >> 8U);
    dstPtr[2] = (unsigned char)(value32 >> 16U);
    dstPtr[3] = (unsigned char)(value32 >> 24U);
}

ui32 ReadLE32(const void* s) {
    const auto srcPtr = static_cast<const unsigned char*>(s);
    ui32 value32 = srcPtr[0];
    value32 += (ui32)srcPtr[1] <<  8U;
    value32 += (ui32)srcPtr[2] << 16U;
    value32 += (ui32)srcPtr[3] << 24U;
    return value32;
}

EStreamType CheckMagic(const void* data) {
    switch (ReadLE32(data)) {
        case Lz4ioMagicNumber:
            return EStreamType::Frame;
        case LegacyMagicNumber:
            return EStreamType::Legacy;
        default:
            return EStreamType::Unknown;
    }
}

EStreamType CheckMagic(NDB::ReadBuffer& input) {
    char data[4u];
    YQL_ENSURE(input.read(data, sizeof(data)) == sizeof(data), "Buffer too small.");
    return CheckMagic(data);
}

}

TReadBuffer::TReadBuffer(NDB::ReadBuffer& source)
    : NDB::ReadBuffer(nullptr, 0ULL), Source(source), StreamType(CheckMagic(Source)), Pos(0ULL), Remaining(0ULL)
{
    YQL_ENSURE(StreamType != EStreamType::Unknown, "Wrong magic.");
    if (StreamType == EStreamType::Frame) {
        const auto errorCode = LZ4F_createDecompressionContext(&Ctx, LZ4F_VERSION);
        YQL_ENSURE(!LZ4F_isError(errorCode), "Can't create LZ4F context resource: " << LZ4F_getErrorName(errorCode));

        InBuffer.resize(64_KB);
        OutBuffer.resize(FrameMaxBlockSize);

        size_t inSize = MagicNumberSize;
        size_t outSize = 0ULL;
        WriteLE32(InBuffer.data(), Lz4ioMagicNumber);

        NextToLoad = LZ4F_decompress_usingDict(Ctx, OutBuffer.data(), &outSize, InBuffer.data(), &inSize, nullptr, 0ULL, nullptr);
        YQL_ENSURE(!LZ4F_isError(NextToLoad), "Header error: " << LZ4F_getErrorName(NextToLoad));
    }
}

TReadBuffer::~TReadBuffer()
{
    if (StreamType == EStreamType::Frame) {
        LZ4F_freeDecompressionContext(Ctx);
    }
}

bool TReadBuffer::nextImpl() {
    switch (StreamType) {
        case EStreamType::Frame:
            if (!NextToLoad)
                return false;

            if (const auto size = DecompressFrame()) {
                working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + size);
                return true;
            }
            break;
        case EStreamType::Legacy:
            if (const auto size = DecompressLegacy()) {
                working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + size);
                return true;
            }
            break;
        default:
            break;
    }

    set(nullptr, 0ULL);
    return false;
}

size_t TReadBuffer::DecompressFrame() {
    while (NextToLoad) {
        if (NextToLoad > InBuffer.size()) {
            InBuffer.resize(NextToLoad);
        }

        if (Pos >= Remaining) {
            for (auto toRead = NextToLoad; toRead > 0U;) {
                const auto sizeCheck = Source.read(InBuffer.data() + NextToLoad - toRead, toRead);
                YQL_ENSURE(sizeCheck > 0U && sizeCheck <= toRead, "Cannot access compressed block.");
                toRead -= sizeCheck;
            }

            Pos = 0ULL;
            Remaining = NextToLoad;
        }

        auto decodedBytes = OutBuffer.size();
        while (Pos < Remaining || (decodedBytes == OutBuffer.size())) {
            decodedBytes = OutBuffer.size();
            NextToLoad = LZ4F_decompress_usingDict(Ctx, OutBuffer.data(), &decodedBytes, InBuffer.data() + Pos, &Remaining, nullptr, 0ULL, nullptr);
            YQL_ENSURE(!LZ4F_isError(NextToLoad), "Decompression error: " << LZ4F_getErrorName(NextToLoad));
            Pos += Remaining;

            if (decodedBytes)
                return decodedBytes;

            if (!NextToLoad)
                return decodedBytes;
        }
    }

    return 0ULL;
}

size_t TReadBuffer::DecompressLegacy() {
    InBuffer.resize(LZ4_compressBound(LegacyBlockSize));
    OutBuffer.resize(LegacyBlockSize);

    unsigned int blockSize = 0U;

    if (const auto sizeCheck = Source.read(InBuffer.data(), 4U)) {
        YQL_ENSURE(sizeCheck == 4U, "Cannot access block size.");
        blockSize = ReadLE32(InBuffer.data());
        YQL_ENSURE(blockSize <= LZ4_COMPRESSBOUND(LegacyBlockSize), "Block size out of bounds.");
    } else
        return 0ULL;

    for (auto toRead = blockSize; toRead > 0U;) {
        const auto sizeCheck = Source.read(InBuffer.data() + blockSize - toRead, toRead);
        YQL_ENSURE(sizeCheck > 0U && sizeCheck <= toRead, "Cannot access compressed block.");
        toRead -= sizeCheck;
    }

    const auto decodeSize = LZ4_decompress_safe(InBuffer.data(), OutBuffer.data(), (int)blockSize, LegacyBlockSize);

    YQL_ENSURE(decodeSize >= 0, "Corrupted input detected.");
    return size_t(decodeSize);
}

namespace {

class TCompressor : public TOutputQueue<> {
public:
    TCompressor(int level)
        : OutputBufferSize(LZ4F_compressFrameBound(FrameMaxBlockSize, nullptr)), OutputBuffer(std::make_unique<char[]>(OutputBufferSize))
    {
        Prefs.autoFlush = 1;
        Prefs.compressionLevel = level;
        Prefs.frameInfo.blockMode = LZ4F_blockMode_t(1);
        Prefs.frameInfo.blockSizeID = LZ4F_blockSizeID_t(7);
        Prefs.frameInfo.blockChecksumFlag = LZ4F_blockChecksum_t(0);
        Prefs.frameInfo.contentSize = 0ULL;
        Prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksum_t(1);
        Prefs.frameInfo.dictID = 0;
        Prefs.favorDecSpeed = 0;

        LZ4F_errorCode_t const errorCode = LZ4F_createCompressionContext(&Ctx, LZ4F_VERSION);
        YQL_ENSURE(!LZ4F_isError(errorCode), "Allocation error: can't create LZ4F context " << LZ4F_getErrorName(errorCode));
    }

    ~TCompressor() {
        if (LZ4F_errorCode_t const errorCode = LZ4F_freeCompressionContext(Ctx); LZ4F_isError(errorCode))
            Cerr << "Error: can't free LZ4F context resource: " << LZ4F_getErrorName(errorCode);
    }

private:
    void Push(TString&& item) override {
        InputQueue.Push(std::move(item));
        DoCompression();
    }

    void Seal() override {
        InputQueue.Seal();
        DoCompression();
    }

    size_t Size() const override {
        return TOutputQueue::Size() + InputQueue.Size();
    }

    bool Empty() const override {
        return TOutputQueue::Empty() && InputQueue.Empty();
    }

    size_t Volume() const override {
        return TOutputQueue::Volume() + InputQueue.Volume();
    }

    void DoCompression() {
        while (!InputQueue.Empty() || !TOutputQueue::IsSealed()) {
            const auto& pop = InputQueue.Pop();
            const bool done = InputQueue.IsSealed() && InputQueue.Empty();
            if (pop.empty() && !done)
                break;

            if (IsFirstBlock && done) {
                const auto cSize = LZ4F_compressFrame_usingCDict(Ctx, OutputBuffer.get(), OutputBufferSize, pop.data(), pop.size(), nullptr, &Prefs);
                YQL_ENSURE(!LZ4F_isError(cSize), "Compression failed: " << LZ4F_getErrorName(cSize));
                TOutputQueue::Push(TString(OutputBuffer.get(), cSize));
                TOutputQueue::Seal();
                IsFirstBlock = false;
            } else {
                if (IsFirstBlock) {
                    const auto headerSize = LZ4F_compressBegin_usingCDict(Ctx, OutputBuffer.get(), OutputBufferSize, nullptr, &Prefs);
                    YQL_ENSURE(!LZ4F_isError(headerSize), "Comression header generation failed: " << LZ4F_getErrorName(headerSize));
                    TOutputQueue::Push(TString(OutputBuffer.get(), headerSize));
                    IsFirstBlock = false;
                }

                if (!pop.empty()) {
                    const auto outSize = LZ4F_compressUpdate(Ctx, OutputBuffer.get(), OutputBufferSize, pop.data(), pop.size(), nullptr);
                    YQL_ENSURE(!LZ4F_isError(outSize), "Compression failed: " << LZ4F_getErrorName(outSize));
                    TOutputQueue::Push(TString(OutputBuffer.get(), outSize));
                }

                if (done) {
                    const auto endSize = LZ4F_compressEnd(Ctx, OutputBuffer.get(), OutputBufferSize, nullptr);
                    YQL_ENSURE(!LZ4F_isError(endSize), "End of frame error: " << LZ4F_getErrorName(endSize));
                    TOutputQueue::Push(TString(OutputBuffer.get(), endSize));
                    TOutputQueue::Seal();
                }
            }
        }
    }

    const std::size_t OutputBufferSize;
    const std::unique_ptr<char[]> OutputBuffer;

    LZ4F_preferences_t Prefs;
    LZ4F_compressionContext_t Ctx;

    TOutputQueue<FrameMaxBlockSize, FrameMaxBlockSize> InputQueue;
    bool IsFirstBlock = true;
};

}

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel) {
    return std::make_unique<TCompressor>(cLevel.value_or(LZ4HC_CLEVEL_DEFAULT));
}

}

}
