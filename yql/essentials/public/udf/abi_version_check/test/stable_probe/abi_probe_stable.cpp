#include "abi_probe_stable.h"

#include <yql/essentials/public/udf/udf_version.h>

ui32 StableProbeAbiVersion() {
    return NYql::NUdf::CurrentAbiVersion();
}
