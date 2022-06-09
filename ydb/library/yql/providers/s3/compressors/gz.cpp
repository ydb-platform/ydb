#include "gz.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

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

        switch (inflate(&Z_, Z_SYNC_FLUSH)) {
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
                ythrow yexception() << GetErrMsg(Z_);
        }
    }
}

}

}
