#pragma once

#include "types.h"
#include "AKAZE.h"

namespace csfm {

py::object akaze(pyarray_uint8 image, AKAZEOptions options);

}
