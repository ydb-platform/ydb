#include "udf_allocator.h"

#include <util/system/compiler.h>

#if UDF_ABI_COMPATIBILITY_VERSION_CURRENT >= UDF_ABI_COMPATIBILITY_VERSION(2, 8)
extern "C" Y_WEAK bool UdfTryFreeExternalString(void* /*mem*/, ui64 /*size*/) {
    return false;
}
#endif
