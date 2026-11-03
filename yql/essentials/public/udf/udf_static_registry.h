#pragma once

#include "udf_registrator.h"

#include <util/generic/vector.h>

namespace NYql::NUdf {

const TVector<const TYqlUdfStaticLibrary*>& GetStaticUdfLibraryList();

} // namespace NYql::NUdf
