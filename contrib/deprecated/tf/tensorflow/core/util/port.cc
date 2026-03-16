/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.

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

#include "tensorflow/core/util/port.h"
#if defined(ARCADIA_BUILD_ROOT) /* #include cpu_id.h and other headers for Yandex */
#include <util/system/cpu_id.h>
#include <cstdlib>
#include <cstring>
#endif /* ARCADIA_BUILD_ROOT */

#if GOOGLE_CUDA
#include "cuda.h" // Y_IGNORE
#endif

namespace tensorflow {

#if defined(ARCADIA_BUILD_ROOT) /* Define IsDanetStylePadding() and other Yandex-specific functions */
static int _IsEnvTrue(const char *name) {
  const char *value = getenv(name);
  if (value && value[0] && (strchr("1yYtT", value[0]) || atoi(value)))
    return 1;
  else
    return 0;
}

bool IsOpencvResize() {
  return _IsEnvTrue("TF_OPENCV_RESIZE");
}
#endif /* ARCADIA_BUILD_ROOT */

bool IsGoogleCudaEnabled() {
#if GOOGLE_CUDA
  return true;
#else
  return false;
#endif
}

bool CudaSupportsHalfMatMulAndConv() {
#if GOOGLE_CUDA
  return true;
#else
  return false;
#endif
}

bool IsMklEnabled() {
#ifdef INTEL_MKL
  return true;
#else
  return false;
#endif
}
}  // end namespace tensorflow
