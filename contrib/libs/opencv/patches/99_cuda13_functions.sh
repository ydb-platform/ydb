#!/bin/bash

# CUDA 13+ compatibility patch for OpenCV CUDA modules
#
# Starting from CUDA 13, stricter checks are enforced that prevent calling
# host functions from device code. Previously, namespace-qualified calls like
# ::fabs(), ::fmax(), ::fmin(), ::exp(), ::atan2() were tolerated in device code,
# but now result in compilation errors such as:
#
# error: calling a __host__ function("fabs(float)") from a __device__ function is not allowed
# error: identifier "fabs" is undefined in device code
#
# This patch automatically replaces these namespace-qualified standard library
# function calls with their device-compatible float variants:
# - ::fabs  -> fabsf
# - ::fmax  -> fmaxf
# - ::fmin  -> fminf
# - ::exp   -> expf
# - ::atan2 -> ::atan2f (keeps namespace prefix for this one)

find modules -type f -name "*.cu" \
  -not -name "absdiff_mat.cu" \
  -not -name "math.cu" \
  -exec sed -i \
  -e 's/::\(fabs\|fmax\|fmin\|exp\) *(/\1f(/g' \
  -e 's/\<\(fabs\|fmax\|fmin\|exp\) *(/\1f(/g' \
  -e 's/::atan2(/::atan2f(/g' \
  {} \;

sed -i \
  -e 's/::\(fabs\|fmax\|fmin\|exp\) *(/\1f(/g' \
  modules/cudev/include/opencv2/cudev/functional/detail/color_cvt.hpp
