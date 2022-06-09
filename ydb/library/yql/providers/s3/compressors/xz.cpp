#include "xz.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

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

}

}
