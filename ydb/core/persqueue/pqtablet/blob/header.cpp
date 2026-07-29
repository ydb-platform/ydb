#include "header.h"
#include <google/protobuf/io/coded_stream.h>
#include <util/generic/buffer.h>
#include <util/system/unaligned_mem.h>
#include <ydb/core/persqueue/public/config.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr {
namespace NPQ {


const ui32 MAX_HEADER_SIZE = 32; // max TBatchHeader size


ui32 GetMaxHeaderSize(const NActors::TActorContext& ctx) {
    const ui32 MAX_HEADER_SIZE_AFTER_BATCHING_ENABLED = 64;
    return IsTopicMessagesBatchingEnabled(ctx) ? MAX_HEADER_SIZE_AFTER_BATCHING_ENABLED : MAX_HEADER_SIZE;
}

NKikimrPQ::TBatchHeader ExtractHeader(const char *data, ui32 size) {
    ui16 sz = ReadUnaligned<ui16>(data);
    AFL_ENSURE(sz < size);
    data += sizeof(ui16);
    NKikimrPQ::TBatchHeader header;
    bool res = header.ParseFromArray(data, sz);
    AFL_ENSURE(res);
    AFL_ENSURE((ui32)header.ByteSize() == sz);

    AFL_ENSURE(header.ByteSize() + header.GetPayloadSize() + sizeof(ui16) <= size);
    return header;
}

}// NPQ
}// NKikimr
