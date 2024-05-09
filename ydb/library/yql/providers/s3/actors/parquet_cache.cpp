#include "parquet_cache.h"

namespace NYql::NDq {

NActors::TActorId ParquetCacheActorId() {
    constexpr TStringBuf name = "PRQTCACH";
    return NActors::TActorId(0, name);
}

NActors::IActor* CreateParquetCache() {
    return nullptr;
}

} // namespace NYql::NDq
