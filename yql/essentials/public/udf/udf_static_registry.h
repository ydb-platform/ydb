#pragma once

#include "udf_registrator.h"

#include <util/generic/vector.h>

namespace NYql {
namespace NUdf {

using TUdfModuleWrapperList = TVector<TUdfModuleWrapper>;
const TUdfModuleWrapperList& GetStaticUdfModuleWrapperList();

}
}
