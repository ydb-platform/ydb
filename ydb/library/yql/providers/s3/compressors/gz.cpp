#include "gz.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include "output_queue_impl.h"

namespace NYql {

namespace NGz {

namespace {

const char* GetErrMsg(const z_stream& z) noexcept {
    return z.msg ? z.msg : "Unknown error.";
}

}

TReadBuffer::TReadBuffer(NDB::ReadBuffer& source)
    : NDB::ReadBuffer(nullptr, 0ULL), Source_(source)
{
    InBuffer.resize(8_KB);
    OutBuffer.resize(64_KB);
    Zero(Z_);
    YQL_ENSURE(inflateInit2(&Z_, 31) == Z_OK, "Can not init inflate engine.");
}

TReadBuffer::~TReadBuffer() {
    inflateEnd(&Z_);
}

bool TReadBuffer::nextImpl() {
    Z_.next_out = reinterpret_cast<unsigned char*>(OutBuffer.data());
    Z_.avail_out = OutBuffer.size();

    while (true) {
        if (!Z_.avail_in) {
            if (const auto size = Source_.read(InBuffer.data(), InBuffer.size())) {
                Z_.next_in = reinterpret_cast<unsigned char*>(InBuffer.data());
                Z_.avail_in = size;
            } else {
                set(nullptr, 0ULL);
                return false;
            }
        }

        switch (const auto code = inflate(&Z_, Z_SYNC_FLUSH)) {
            case Z_NEED_DICT:
                ythrow yexception() << "Need dict.";
            case Z_STREAM_END:
                YQL_ENSURE(inflateReset(&Z_) == Z_OK, "Inflate reset error: " << GetErrMsg(Z_));
                [[fallthrough]];
            case Z_OK:
                if (const auto processed = OutBuffer.size() - Z_.avail_out) {
                    working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + processed);
                    return true;
                }
                break;
            default:
                ythrow yexception() << GetErrMsg(Z_) << ", code: " << code;
        }
    }
}

namespace {

class TCompressor : public TOutputQueue<> {
public:
    TCompressor(int level) {
        Zero(Z_);
        YQL_ENSURE(deflateInit2(&Z_, level, Z_DEFLATED, 16 | MAX_WBITS, MAX_MEM_LEVEL, Z_DEFAULT_STRATEGY) == Z_OK, "Can not init deflate engine.");
    }

    ~TCompressor() {
        deflateEnd(&Z_);
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

            Z_.next_in = const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(pop.data()));
            Z_.avail_in = pop.size();

            if (const auto bound = deflateBound(&Z_, Z_.avail_in); bound > OutputBufferSize)
                OutputBuffer = std::make_unique<char[]>(OutputBufferSize = bound);

            Z_.next_out = reinterpret_cast<unsigned char*>(OutputBuffer.get());
            Z_.avail_out = OutputBufferSize;

            const auto code = deflate(&Z_, done ? Z_FINISH : Z_BLOCK);
            YQL_ENSURE((done ? Z_STREAM_END : Z_OK) == code, "code: " << code << ", error: " << GetErrMsg(Z_));

            if (const auto size = OutputBufferSize - Z_.avail_out)
                TOutputQueue::Push(TString(OutputBuffer.get(), size));

            if (done)
                return TOutputQueue::Seal();
        };
    }

    std::size_t OutputBufferSize = 0ULL;
    std::unique_ptr<char[]> OutputBuffer;

    z_stream Z_;

    TOutputQueue<0> InputQueue;
};

}

IOutputQueue::TPtr MakeCompressor(std::optional<int> cLevel) {
    return std::make_unique<TCompressor>(cLevel.value_or(Z_DEFAULT_COMPRESSION));
}

}

}
