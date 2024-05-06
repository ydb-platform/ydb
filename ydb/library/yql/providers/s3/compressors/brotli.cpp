#include "brotli.h"
#include "output_queue_impl.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <contrib/libs/brotli/include/brotli/encode.h>

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
            InputSize_ = InputAvailable_;
            if (InputAvailable_ == 0) {
                InputExhausted_ = true;
            }
        }

        if (SubstreamFinished_ && !InputExhausted_) {
            FreeDecoder();
            InitDecoder();
        }

        auto inBuffer = const_cast<const unsigned char*>(reinterpret_cast<unsigned char*>(InBuffer.data()) + (InputSize_ - InputAvailable_));
        auto outBuffer = reinterpret_cast<unsigned char*>(OutBuffer.data()) + (OutBuffer.size() - availableOut);

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

namespace {

class TCompressor : public TOutputQueue<> {
public:
    TCompressor(int quiality)
        : EncoderState_(BrotliEncoderCreateInstance(&TAllocator::Allocate, &TAllocator::Deallocate, nullptr)), Quiality_(quiality) {
        YQL_ENSURE(EncoderState_, "Brotli encoder initialization failed.");
        YQL_ENSURE(BrotliEncoderSetParameter(EncoderState_, BROTLI_PARAM_QUALITY, Quiality_), "Failed to set quility: " << Quiality_);
        YQL_ENSURE(BrotliEncoderSetParameter(EncoderState_, BROTLI_PARAM_LGWIN, BROTLI_DEFAULT_WINDOW), "Failed to set window bits.");
        YQL_ENSURE(BrotliEncoderSetParameter(EncoderState_, BROTLI_PARAM_MODE, BROTLI_DEFAULT_MODE), "Failed to set mode.");
    }

    ~TCompressor() {
        BrotliEncoderDestroyInstance(EncoderState_);
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

            size_t input_size = pop.size();
            auto input_data = reinterpret_cast<const uint8_t*>(pop.data());
            if (const auto bound = BrotliEncoderMaxCompressedSize(input_size); bound > OutputBufferSize)
                OutputBuffer = std::make_unique<char[]>(OutputBufferSize = bound);

            auto output_data = reinterpret_cast<uint8_t*>(OutputBuffer.get());
            auto output_size = OutputBufferSize;

            if (IsFirstBlock && done) {
                YQL_ENSURE(BrotliEncoderCompress(Quiality_, BROTLI_DEFAULT_WINDOW, BROTLI_DEFAULT_MODE, input_size, input_data, &output_size, output_data), "Encode failed.");
                if (output_size)
                    TOutputQueue::Push(TString(OutputBuffer.get(), output_size));
            } else {
                size_t total_size = 0ULL;
                YQL_ENSURE(BrotliEncoderCompressStream(EncoderState_, done ? BROTLI_OPERATION_FINISH : BROTLI_OPERATION_PROCESS, &input_size, &input_data, &output_size, &output_data, &total_size), "Encode failed.");
                if (const auto size = OutputBufferSize - output_size)
                    TOutputQueue::Push(TString(OutputBuffer.get(), size));

            }

            IsFirstBlock = false;
            if (done)
                return TOutputQueue::Seal();
        };
    }

    std::size_t OutputBufferSize = 0ULL;
    std::unique_ptr<char[]> OutputBuffer;

    BrotliEncoderState *const EncoderState_;
    const int Quiality_;

    TOutputQueue<0> InputQueue;
    bool IsFirstBlock = true;
};

}
IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel) {
    return std::make_unique<TCompressor>(cLevel.value_or(BROTLI_DEFAULT_QUALITY));
}

}

}
