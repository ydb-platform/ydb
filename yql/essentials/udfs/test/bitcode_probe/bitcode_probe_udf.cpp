#include "bitcode_probe_ir.h"

#include <yql/essentials/public/udf/udf_helpers.h>

using namespace NKikimr;
using namespace NUdf;

namespace {

#ifdef DISABLE_IR
SIMPLE_STRICT_UDF(TTwice, double(TAutoMap<double>)) {
    TUnboxedValuePod result;
    TwiceIR(this, &result, valueBuilder, args);
    return result;
}
#else
SIMPLE_STRICT_UDF_WITH_IR(TTwice, double(TAutoMap<double>), 0, "/llvm_bc/BitcodeProbe", "TwiceIR") {
    TUnboxedValuePod result;
    TwiceIR(this, &result, valueBuilder, args);
    return result;
}
#endif

SIMPLE_MODULE(TBitcodeProbeModule,
              TTwice)

} // namespace

REGISTER_MODULES(TBitcodeProbeModule)
