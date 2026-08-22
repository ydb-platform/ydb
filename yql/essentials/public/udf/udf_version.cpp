#include "udf_version.h"

#include <util/string/builder.h>

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
