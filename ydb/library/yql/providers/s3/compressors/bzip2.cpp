#include "bzip2.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include "output_queue_impl.h"

namespace NYql {

namespace NBzip2 {

TReadBuffer::TReadBuffer(NDB::ReadBuffer& source)
    : NDB::ReadBuffer(nullptr, 0ULL), Source_(source)
{
    InBuffer.resize(8_KB);
    OutBuffer.resize(64_KB);
    Zero(BzStream_);
    InitDecoder();
}

TReadBuffer::~TReadBuffer() {
    FreeDecoder();
}

void TReadBuffer::InitDecoder() {
    YQL_ENSURE(BZ2_bzDecompressInit(&BzStream_, 0, 0) == BZ_OK, "Can not init bzip engine.");
}

void TReadBuffer::FreeDecoder() {
    BZ2_bzDecompressEnd(&BzStream_);
}

bool TReadBuffer::nextImpl() {
    BzStream_.next_out = OutBuffer.data();
    BzStream_.avail_out = OutBuffer.size();

    while (true) {
        if (!BzStream_.avail_in) {
            if (const auto size = Source_.read(InBuffer.data(), InBuffer.size())) {
                BzStream_.next_in = InBuffer.data();
                BzStream_.avail_in = size;
            } else {
                set(nullptr, 0ULL);
                return false;
            }
        }

        switch (const auto code = BZ2_bzDecompress(&BzStream_)) {
            case BZ_STREAM_END:
                FreeDecoder();
                InitDecoder();
                [[fallthrough]];
            case BZ_OK:
                if (const auto processed = OutBuffer.size() - BzStream_.avail_out) {
                    working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + processed);
                    return true;
                }

                break;
            default:
                ythrow yexception() << "Bzip error: " << code;
        }
    }
}

namespace {

class TCompressor : public TOutputQueue<> {
public:
    TCompressor(int blockSize100k) {
        Zero(BzStream_);
        YQL_ENSURE(BZ2_bzCompressInit(&BzStream_, blockSize100k, 0, 30) == BZ_OK, "Can not init deflate engine.");
    }

    ~TCompressor() {
        BZ2_bzCompressEnd(&BzStream_);
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

            if (const auto bound = BZ_MAX_UNUSED + pop.size(); bound > OutputBufferSize)
                OutputBuffer = std::make_unique<char[]>(OutputBufferSize = bound);

            BzStream_.next_in = const_cast<char*>(pop.data());
            BzStream_.avail_in = pop.size();

            BzStream_.next_out = OutputBuffer.get();
            BzStream_.avail_out = OutputBufferSize;

            const auto code = BZ2_bzCompress(&BzStream_, done ? BZ_FINISH : BZ_RUN);
            YQL_ENSURE((done ? BZ_STREAM_END : BZ_RUN_OK) == code, "Bzip error code: " << code);

            if (const auto size = OutputBufferSize - BzStream_.avail_out)
                TOutputQueue::Push(TString(OutputBuffer.get(), size));

            if (done)
                return TOutputQueue::Seal();
        };
    }

    std::size_t OutputBufferSize = 0ULL;
    std::unique_ptr<char[]> OutputBuffer;

    bz_stream BzStream_;

    TOutputQueue<0> InputQueue;
};

}

IOutputQueue::TPtr MakeCompressor(std::optional<int> blockSize100k) {
    return std::make_unique<TCompressor>(blockSize100k.value_or(9));
}

}

}
