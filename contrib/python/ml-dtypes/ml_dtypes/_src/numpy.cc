/* Copyright 2022 The ml_dtypes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

// We define the PY_ARRAY_UNIQUE_SYMBOL in this .cc file and provide an
// ImportNumpy function to populate it.
#define ML_DTYPES_IMPORT_NUMPY

#include "_src/numpy.h"

namespace ml_dtypes {

void ImportNumpy() { import_array1(); }

}  // namespace ml_dtypes
