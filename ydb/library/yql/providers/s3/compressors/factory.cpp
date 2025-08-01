#include "factory.h"

#if defined(_win_)

namespace NYql {

IOutputQueue::TPtr MakeCompressorQueue(const std::string_view& /*compression*/) {
    return {};
}

} // namespace NYql

#else

#include "lz4io.h"
#include "brotli.h"
#include "zstd.h"
#include "bzip2.h"
#include "xz.h"
#include "gz.h"
#include "output_queue_impl.h"

#include <ydb/library/yql/udfs/common/clickhouse/client/src/IO/ReadBuffer.h>

namespace NYql {

std::unique_ptr<NDB::ReadBuffer> MakeDecompressor(NDB::ReadBuffer& input, const std::string_view& compression) {
    if ("lz4" == compression) {
        return NLz4::MakeDecompressor(input);
    }
    if ("brotli" == compression) {
        return NBrotli::MakeDecompressor(input);
    }
    if ("zstd" == compression) {
        return NZstd::MakeDecompressor(input);
    }
    if ("bzip2" == compression) {
        return NBzip2::MakeDecompressor(input);
    }
    if ("xz" == compression) {
        return NXz::MakeDecompressor(input);
    }
    if ("gzip" == compression) {
        return NGz::MakeDecompressor(input);
    }

    return nullptr;
}

IOutputQueue::TPtr MakeCompressorQueue(const std::string_view& compression) {
    if ("lz4" == compression) {
        return NLz4::MakeCompressor();
    }
    if ("brotli" == compression) {
        return NBrotli::MakeCompressor();
    }
    if ("zstd" == compression) {
        return  NZstd::MakeCompressor();
    }
    if ("bzip2" == compression) {
        return  NBzip2::MakeCompressor();
    }
    if ("xz" == compression) {
        return NXz::MakeCompressor();
    }
    if ("gzip" == compression) {
        return NGz::MakeCompressor();
    }
    if (compression.empty()) {
        return std::make_unique<TOutputQueue<5_MB>>();
    }

    return {};
}

} // namespace NYql

#endif
