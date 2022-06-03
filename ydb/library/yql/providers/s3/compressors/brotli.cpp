#include "brotli.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

namespace NBrotli {

namespace {
    struct TAllocator {
        static void* Allocate(void* /* opaque */, size_t size) {
            return ::operator new(size);
        }

        static void Deallocate(void* /* opaque */, void* ptr) noexcept {
            ::operator delete(ptr);
        }
    };

}

TReadBuffer::TReadBuffer(NDB::ReadBuffer& source)
    : NDB::ReadBuffer(nullptr, 0ULL), Source_(source)
{
    InBuffer.resize(8_KB);
    OutBuffer.resize(64_KB);
    InitDecoder();
}

TReadBuffer::~TReadBuffer() {
    FreeDecoder();
}

bool TReadBuffer::nextImpl() {
    auto availableOut = OutBuffer.size();
    size_t decompressedSize = 0;

    BrotliDecoderResult result;
    do {
        if (InputAvailable_ == 0 && !InputExhausted_) {
            InputAvailable_ = Source_.read(reinterpret_cast<char*>(InBuffer.data()), InBuffer.size());
            if (InputAvailable_ == 0) {
                InputExhausted_ = true;
            }
        }

        if (SubstreamFinished_ && !InputExhausted_) {
            FreeDecoder();
            InitDecoder();
        }

        auto inBuffer = const_cast<const unsigned char*>(reinterpret_cast<unsigned char*>(InBuffer.data()));
        auto outBuffer = reinterpret_cast<unsigned char*>(OutBuffer.data());

        SubstreamFinished_ = false;
        result = BrotliDecoderDecompressStream(
            DecoderState_,
            &InputAvailable_,
            &inBuffer,
            &availableOut,
            &outBuffer,
            nullptr);

        decompressedSize = OutBuffer.size() - availableOut;
        switch (result) {
            case BROTLI_DECODER_RESULT_SUCCESS:
                SubstreamFinished_ = true;
                break;
            case BROTLI_DECODER_RESULT_ERROR:
                ythrow yexception() << "Brotli decoder failed to decompress buffer: "
                                    << BrotliDecoderErrorString(BrotliDecoderGetErrorCode(DecoderState_));
            case BROTLI_DECODER_RESULT_NEEDS_MORE_OUTPUT:
                YQL_ENSURE(availableOut != OutBuffer.size(), "Buffer passed to read in Brotli decoder is too small");
                break;
            default:
                break;
        }

    } while (!decompressedSize && result == BROTLI_DECODER_RESULT_NEEDS_MORE_INPUT && !InputExhausted_);

    if (!SubstreamFinished_ && !decompressedSize) {
        ythrow yexception() << "Input stream is incomplete";
    }

    if (decompressedSize > 0)
        working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + decompressedSize);
    else
        set(nullptr, 0ULL);

    return decompressedSize > 0;
}

void TReadBuffer::InitDecoder() {
    DecoderState_ = BrotliDecoderCreateInstance(&TAllocator::Allocate, &TAllocator::Deallocate, nullptr);
    if (!DecoderState_) {
        ythrow yexception() << "Brotli decoder initialization failed";
    }
}

void TReadBuffer::FreeDecoder() {
    BrotliDecoderDestroyInstance(DecoderState_);
}

}

}
