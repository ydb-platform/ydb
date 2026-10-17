#include "abi_probe_current.h"

#include <yql/essentials/public/udf/udf_version.h>

ui32 CurrentProbeAbiVersion() {
    return NYql::NUdf::CurrentAbiVersion();
}
