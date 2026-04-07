#pragma once

#include "udf_registrator.h"

#include <util/generic/vector.h>

namespace NYql::NUdf {

using TUdfModuleWrapperList = TVector<TUdfModuleWrapper>;
const TUdfModuleWrapperList& GetStaticUdfModuleWrapperList();

} // namespace NYql::NUdf
