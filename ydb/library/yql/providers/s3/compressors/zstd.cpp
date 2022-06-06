#include "zstd.h"

#include <util/generic/size_literals.h>
#include <ydb/library/yql/utils/yql_panic.h>

namespace NYql {

namespace NZstd {

TReadBuffer::TReadBuffer(NDB::ReadBuffer& source)
    : NDB::ReadBuffer(nullptr, 0ULL), Source_(source), ZCtx_(::ZSTD_createDStream())
{
    InBuffer.resize(8_KB);
    OutBuffer.resize(64_KB);
    Offset_ = InBuffer.size();
}

TReadBuffer::~TReadBuffer() {
    ::ZSTD_freeDStream(ZCtx_);
}

bool TReadBuffer::nextImpl() {
    ::ZSTD_inBuffer zIn{InBuffer.data(), InBuffer.size(), Offset_};
    ::ZSTD_outBuffer zOut{OutBuffer.data(), OutBuffer.size(), 0ULL};

    size_t returnCode = 0ULL;
    if (!Finished_) do {
        if (zIn.pos == zIn.size) {
            zIn.size = Source_.read(InBuffer.data(), InBuffer.size());

            zIn.pos = Offset_ = 0;
            if (!zIn.size) {
                // end of stream, need to check that there is no uncompleted blocks
                YQL_ENSURE(!returnCode, "Incomplete block.");
                Finished_ = true;
                break;
            }
        }
        returnCode = ::ZSTD_decompressStream(ZCtx_, &zOut, &zIn);
        YQL_ENSURE(!::ZSTD_isError(returnCode), "Decompress failed: " << ::ZSTD_getErrorName(returnCode));
        if (!returnCode) {
            // The frame is over, prepare to (maybe) start a new frame
            ::ZSTD_initDStream(ZCtx_);
        }
    } while (zOut.pos != zOut.size);
    Offset_ = zIn.pos;

    if (zOut.pos) {
        working_buffer = Buffer(OutBuffer.data(), OutBuffer.data() + zOut.pos);
        return true;
    } else {
        set(nullptr, 0ULL);
        return false;
    }
}

}

}
