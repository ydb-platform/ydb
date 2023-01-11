#include "xz.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include "output_queue_impl.h"

namespace NYql {

namespace NXz {

TReadBuffer::TReadBuffer(NDB::ReadBuffer& source)
    : NDB::ReadBuffer(nullptr, 0ULL), Source_(source), Strm_(LZMA_STREAM_INIT)
{
    InBuffer.resize(8_KB);
    OutBuffer.resize(64_KB);

    switch (const lzma_ret ret = lzma_auto_decoder(&Strm_, UINT64_MAX, LZMA_CONCATENATED)) {
        case LZMA_OK:
            return;
        case LZMA_MEM_ERROR:
            throw yexception() << "Memory allocation failed.";
        case LZMA_OPTIONS_ERROR:
            throw yexception() << "Unsupported decompressor flags.";
        default:
            throw yexception() << "Unknown error << " << int(ret) << ", possibly a bug.";
    }
}

TReadBuffer::~TReadBuffer() {
    lzma_end(&Strm_);
}

bool TReadBuffer::nextImpl() {
    if (IsOutFinished_) {
        return false;
    }

    lzma_action action = LZMA_RUN;

    Strm_.next_out = reinterpret_cast<unsigned char*>(OutBuffer.data());
    Strm_.avail_out = OutBuffer.size();

    while (true) {
        if (!Strm_.avail_in && !IsInFinished_) {
            if (const auto size = Source_.read(InBuffer.data(), InBuffer.size())) {
                Strm_.next_in = reinterpret_cast<unsigned char*>(InBuffer.data());
                Strm_.avail_in = size;
            } else {
                IsInFinished_ = true;
                action = LZMA_FINISH;
            }
        }

        const lzma_ret ret = lzma_code(&Strm_, action);
        if (ret == LZMA_STREAM_END) {
            IsOutFinished_ = true;
        }

        if (!Strm_.avail_out || ret == LZMA_STREAM_END) {
            if (const auto outLen = OutBuffer.size() - Strm_.avail_out) {
                working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + outLen);
                return true;
            } else {
                set(nullptr, 0ULL);
                return false;
            }
        }

        switch (ret) {
            case LZMA_OK:
                continue;
            case LZMA_MEM_ERROR:
                throw yexception() << "Memory allocation failed.";
            case LZMA_FORMAT_ERROR:
                throw yexception() << "The input is not in the .xz format.";
            case LZMA_OPTIONS_ERROR:
                throw yexception() << "Unsupported compression options.";
            case LZMA_DATA_ERROR:
                throw yexception() << "Compressed file is corrupt.";
            case LZMA_BUF_ERROR:
                throw yexception() << "Compressed file is truncated or otherwise corrupt.";
            default:
                throw yexception() << "Unknown error " << int(ret) << ", possibly a bug.";
        }
    }
}

namespace {

class TCompressor : public TOutputQueue<> {
public:
    TCompressor(int level) : Strm_(LZMA_STREAM_INIT) {


    // options for further compression
    lzma_options_lzma opt_lzma2;
    if (lzma_lzma_preset(&opt_lzma2, level))
        throw yexception() << "lzma preset failed: lzma version: " << LZMA_VERSION_STRING;

    lzma_filter filters[] = {
        {.id = LZMA_FILTER_X86, .options = nullptr},
        {.id = LZMA_FILTER_LZMA2, .options = &opt_lzma2},
        {.id = LZMA_VLI_UNKNOWN, .options = nullptr},
    };

    switch (const lzma_ret ret = lzma_stream_encoder(&Strm_, filters, LZMA_CHECK_CRC64)) {
            case LZMA_OK:
                return;
            case LZMA_MEM_ERROR:
                throw yexception() << "Memory allocation failed.";
            case LZMA_OPTIONS_ERROR:
                throw yexception() << "Unsupported decompressor flags.";
            default:
                throw yexception() << "Unknown error << " << int(ret) << ", possibly a bug.";
        }
    }

    ~TCompressor() {
        lzma_end(&Strm_);
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

            if (const auto bound = lzma_stream_buffer_bound(pop.size()); bound > OutputBufferSize)
                OutputBuffer = std::make_unique<char[]>(OutputBufferSize = bound);

            Strm_.next_in = const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(pop.data()));
            Strm_.avail_in = pop.size();

            Strm_.next_out = reinterpret_cast<unsigned char*>(OutputBuffer.get());
            Strm_.avail_out = OutputBufferSize;

            const lzma_ret ret = lzma_code(&Strm_, done ? LZMA_FINISH : LZMA_RUN);

            if (const auto size = OutputBufferSize - Strm_.avail_out)
                TOutputQueue::Push(TString(OutputBuffer.get(), size));

            switch (ret) {
                case LZMA_OK:
                    continue;
                case LZMA_STREAM_END:
                    return TOutputQueue::Seal();
                case LZMA_MEM_ERROR:
                    throw yexception() << "Memory allocation failed.";
                case LZMA_FORMAT_ERROR:
                    throw yexception() << "The input is not in the .xz format.";
                case LZMA_OPTIONS_ERROR:
                    throw yexception() << "Unsupported compression options.";
                case LZMA_DATA_ERROR:
                    throw yexception() << "Compressed file is corrupt.";
                case LZMA_BUF_ERROR:
                    throw yexception() << "Compressed file is truncated or otherwise corrupt.";
                default:
                    throw yexception() << "Unknown error " << int(ret) << ", possibly a bug.";
            }
        };
    }

    std::size_t OutputBufferSize = 0ULL;
    std::unique_ptr<char[]> OutputBuffer;

    lzma_stream Strm_;

    TOutputQueue<0> InputQueue;
};

}

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel) {
    return std::make_unique<TCompressor>(cLevel.value_or(LZMA_PRESET_DEFAULT));
}

}

}
