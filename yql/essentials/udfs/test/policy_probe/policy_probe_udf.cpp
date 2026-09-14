#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

// Allocating is the whole point of the function: the memory has to come from the host that loaded
// the udf. The stub policy this udf peers aborts in UdfAllocateWithSize, so if its GLOBAL object
// ever ends up linked in here, the call below dies instead of returning a string.
SIMPLE_UDF(TEcho, char*(TAutoMap<char*>)) {
    return valueBuilder->NewString(args[0].AsStringRef());
}

SIMPLE_MODULE(TPolicyProbeModule,
              TEcho)

} // namespace

REGISTER_MODULES(TPolicyProbeModule)
