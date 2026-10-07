#include "abi_version_check.h"

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <cstdlib>

namespace NYql::NUdf {

TString AbiVersionToStr(ui32 version)
{
    TStringBuilder sb;
    sb << (version / 10000) << '.'
       << (version / 100) % 100 << '.'
       << (version % 100);

    return sb;
}

} // namespace NYql::NUdf

extern "C" ui32 YqlCheckAbiVersion(ui32 loaded, ui32 built) {
    if (loaded != 0 && loaded != built) {
        Cerr << "Mismatch YQL ABI versions in a single binary: "
             << ::NYql::NUdf::AbiVersionToStr(loaded) << " != " << ::NYql::NUdf::AbiVersionToStr(built) << Endl;

        abort();
    }

    return built;
}
