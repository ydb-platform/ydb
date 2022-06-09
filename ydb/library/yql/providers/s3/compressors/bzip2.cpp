#include "bzip2.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

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

}

}
