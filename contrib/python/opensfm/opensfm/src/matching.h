#pragma once

#include "types.h"

namespace csfm {

py::object match_using_words(pyarray_f features1,
                             pyarray_int words1,
                             pyarray_f features2,
                             pyarray_int words2,
                             float lowes_ratio,
                             int max_checks);

}
