#include "events.h"

// Clickhouse includes MUST be after all other includes due to sanitizer defines like THREAD_SANITIZER, it may affect other libs like protobufs
#include <ydb/library/yql/udfs/common/clickhouse/client/src/Core/Block.h>

namespace NYql::NDq {

TEvS3Provider::TEvNextBlock::TEvNextBlock(NDB::Block& block, size_t pathInd, ui64 ingressDelta, TDuration cpuTimeDelta, ui64 ingressDecompressedDelta)
    : Block(std::make_unique<NDB::Block>())
    , PathIndex(pathInd)
    , IngressDelta(ingressDelta)
    , CpuTimeDelta(cpuTimeDelta)
    , IngressDecompressedDelta(ingressDecompressedDelta)
{
    Block->swap(block);
}

} // namespace NYql::NDq
