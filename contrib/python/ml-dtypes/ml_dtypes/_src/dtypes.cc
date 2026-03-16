/* Copyright 2017 The ml_dtypes Authors.

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

// Enable cmath defines on Windows
#define _USE_MATH_DEFINES

// Must be included first
// clang-format off
#include "_src/numpy.h" //NOLINT
// clang-format on

#include <array>    // NOLINT
#include <cmath>    // NOLINT
#include <cstdint>  // NOLINT
#include <limits>   // NOLINT
#include <locale>   // NOLINT

// Place `<locale>` before <Python.h> to avoid a build failure in macOS.
#include <Python.h>

#include "Eigen/Core"
#include "_src/custom_float.h"
#include "_src/intn_numpy.h"
#include "include/float8.h"
#include "include/intn.h"
#include "include/mxfloat.h"

namespace ml_dtypes {

using bfloat16 = Eigen::bfloat16;

template <>
struct TypeDescriptor<bfloat16> : CustomFloatType<bfloat16> {
  typedef bfloat16 T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "bfloat16";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.bfloat16";
  static constexpr const char* kTpDoc = "bfloat16 floating-point values";
  // We must register bfloat16 with a kind other than "f", because numpy
  // considers two types with the same kind and size to be equal, but
  // float16 != bfloat16.
  // The downside of this is that NumPy scalar promotion does not work with
  // bfloat16 values.
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'E';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float8_e3m4> : CustomFloatType<float8_e3m4> {
  typedef float8_e3m4 T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e3m4";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e3m4";
  static constexpr const char* kTpDoc = "float8_e3m4 floating-point values";
  // Set e3m4 kind as Void since kind=f (float) with itemsize=1 is used by e5m2
  static constexpr char kNpyDescrKind = 'V';  // Void
  static constexpr char kNpyDescrType = '3';
  static constexpr char kNpyDescrByteorder = '=';  // Native byte order
};

template <>
struct TypeDescriptor<float8_e4m3> : CustomFloatType<float8_e4m3> {
  typedef float8_e4m3 T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e4m3";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e4m3";
  static constexpr const char* kTpDoc = "float8_e4m3 floating-point values";
  // Set e4m3 kind as Void since kind=f (float) with itemsize=1 is used by e5m2
  static constexpr char kNpyDescrKind = 'V';       // Void
  static constexpr char kNpyDescrType = '7';       // '4' is reserved for e4m3fn
  static constexpr char kNpyDescrByteorder = '=';  // Native byte order
};

template <>
struct TypeDescriptor<float8_e4m3b11fnuz>
    : CustomFloatType<float8_e4m3b11fnuz> {
  typedef float8_e4m3b11fnuz T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e4m3b11fnuz";
  static constexpr const char* kQualifiedTypeName =
      "ml_dtypes.float8_e4m3b11fnuz";
  static constexpr const char* kTpDoc =
      "float8_e4m3b11fnuz floating-point values";
  // We must register float8_e4m3b11fnuz with a kind other than "f", because
  // numpy considers two types with the same kind and size to be equal, and we
  // expect multiple 1 byte floating point types.
  // The downside of this is that NumPy scalar promotion does not work with
  // float8_e4m3b11fnuz values.
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'L';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float8_e4m3fn> : CustomFloatType<float8_e4m3fn> {
  typedef float8_e4m3fn T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e4m3fn";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e4m3fn";
  static constexpr const char* kTpDoc = "float8_e4m3fn floating-point values";
  // We must register float8_e4m3fn with a unique kind, because numpy
  // considers two types with the same kind and size to be equal.
  // The downside of this is that NumPy scalar promotion does not work with
  // float8 values.  Using 'V' to mirror bfloat16 vs float16.
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = '4';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float8_e4m3fnuz> : CustomFloatType<float8_e4m3fnuz> {
  typedef float8_e4m3fnuz T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e4m3fnuz";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e4m3fnuz";
  static constexpr const char* kTpDoc = "float8_e4m3fnuz floating-point values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'G';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float8_e5m2> : CustomFloatType<float8_e5m2> {
  typedef float8_e5m2 T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e5m2";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e5m2";
  static constexpr const char* kTpDoc = "float8_e5m2 floating-point values";
  // Treating e5m2 as the natural "float" type since it is IEEE-754 compliant.
  static constexpr char kNpyDescrKind = 'f';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = '5';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float8_e5m2fnuz> : CustomFloatType<float8_e5m2fnuz> {
  typedef float8_e5m2fnuz T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e5m2fnuz";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e5m2fnuz";
  static constexpr const char* kTpDoc = "float8_e5m2fnuz floating-point values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'C';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float6_e2m3fn> : CustomFloatType<float6_e2m3fn> {
  typedef float6_e2m3fn T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float6_e2m3fn";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float6_e2m3fn";
  static constexpr const char* kTpDoc = "float6_e2m3fn floating-point values";
  static constexpr char kNpyDescrKind = 'V';
  static constexpr char kNpyDescrType = '8';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float6_e3m2fn> : CustomFloatType<float6_e3m2fn> {
  typedef float6_e3m2fn T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float6_e3m2fn";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float6_e3m2fn";
  static constexpr const char* kTpDoc = "float6_e3m2fn floating-point values";
  static constexpr char kNpyDescrKind = 'V';
  static constexpr char kNpyDescrType = '9';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float4_e2m1fn> : CustomFloatType<float4_e2m1fn> {
  typedef float4_e2m1fn T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float4_e2m1fn";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float4_e2m1fn";
  static constexpr const char* kTpDoc = "float4_e2m1fn floating-point values";
  static constexpr char kNpyDescrKind = 'V';
  static constexpr char kNpyDescrType = '0';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<float8_e8m0fnu> : CustomFloatType<float8_e8m0fnu> {
  typedef float8_e8m0fnu T;
  static constexpr bool is_floating = true;
  static constexpr bool is_integral = false;
  static constexpr const char* kTypeName = "float8_e8m0fnu";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.float8_e8m0fnu";
  static constexpr const char* kTpDoc = "float8_e8m0fnu floating-point values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'W';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<int2> : IntNTypeDescriptor<int2> {
  typedef int2 T;
  static constexpr bool is_floating = false;
  static constexpr bool is_integral = true;
  static constexpr const char* kTypeName = "int2";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.int2";
  static constexpr const char* kTpDoc = "int2 integer values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'c';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<uint2> : IntNTypeDescriptor<uint2> {
  typedef uint2 T;
  static constexpr bool is_floating = false;
  static constexpr bool is_integral = true;
  static constexpr const char* kTypeName = "uint2";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.uint2";
  static constexpr const char* kTpDoc = "uint2 integer values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'C';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<int4> : IntNTypeDescriptor<int4> {
  typedef int4 T;
  static constexpr bool is_floating = false;
  static constexpr bool is_integral = true;
  static constexpr const char* kTypeName = "int4";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.int4";
  static constexpr const char* kTpDoc = "int4 integer values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'a';
  static constexpr char kNpyDescrByteorder = '=';
};

template <>
struct TypeDescriptor<uint4> : IntNTypeDescriptor<uint4> {
  typedef uint4 T;
  static constexpr bool is_floating = false;
  static constexpr bool is_integral = true;
  static constexpr const char* kTypeName = "uint4";
  static constexpr const char* kQualifiedTypeName = "ml_dtypes.uint4";
  static constexpr const char* kTpDoc = "uint4 integer values";
  static constexpr char kNpyDescrKind = 'V';
  // TODO(phawkins): there doesn't seem to be a way of guaranteeing a type
  // character is unique.
  static constexpr char kNpyDescrType = 'A';
  static constexpr char kNpyDescrByteorder = '=';
};

namespace {

// Performs a NumPy array cast from type 'From' to 'To' via `Via`.
template <typename From, typename To, typename Via>
void PyCast(void* from_void, void* to_void, npy_intp n, void* fromarr,
            void* toarr) {
  const auto* from = static_cast<From*>(from_void);
  auto* to = static_cast<To*>(to_void);
  for (npy_intp i = 0; i < n; ++i) {
    to[i] = static_cast<To>(static_cast<Via>(from[i]));
  }
}

template <typename Type1, typename Type2, typename Via>
bool RegisterTwoWayCustomCast() {
  int nptype1 = TypeDescriptor<Type1>::npy_type;
  int nptype2 = TypeDescriptor<Type2>::npy_type;
  PyArray_Descr* descr1 = PyArray_DescrFromType(nptype1);
  if (PyArray_RegisterCastFunc(descr1, nptype2, PyCast<Type1, Type2, Via>) <
      0) {
    return false;
  }
  PyArray_Descr* descr2 = PyArray_DescrFromType(nptype2);
  if (PyArray_RegisterCastFunc(descr2, nptype1, PyCast<Type2, Type1, Via>) <
      0) {
    return false;
  }
  return true;
}

template <typename Type1, typename Type2, typename Via>
bool RegisterOneWayCustomCast() {
  int nptype1 = TypeDescriptor<Type1>::npy_type;
  int nptype2 = TypeDescriptor<Type2>::npy_type;
  PyArray_Descr* descr1 = PyArray_DescrFromType(nptype1);
  if (PyArray_RegisterCastFunc(descr1, nptype2, PyCast<Type1, Type2, Via>) <
      0) {
    return false;
  }
  return true;
}

// Register two-way floating point casts between the first and the other types.
template <typename T>
bool RegisterTwoWayFloatCasts() {
  return true;
}

template <typename T, typename U, typename... Args>
bool RegisterTwoWayFloatCasts() {
  return RegisterTwoWayCustomCast<T, U, float>() &&
         RegisterTwoWayFloatCasts<T, Args...>();
}

// Register two-way floating point casts between all pairs of types.
template <typename T>
bool RegisterAllFloatCasts() {
  return true;
}

template <typename T, typename U, typename... Args>
bool RegisterAllFloatCasts() {
  return RegisterTwoWayFloatCasts<T, U, Args...>() &&
         RegisterAllFloatCasts<U, Args...>();
}

// Initialize type attribute in the module object.
template <typename T>
bool InitModuleType(PyObject* obj, const char* name) {
  return PyObject_SetAttrString(
             obj, name,
             reinterpret_cast<PyObject*>(TypeDescriptor<T>::type_ptr)) >= 0;
}

}  // namespace

// Initializes the module.
bool Initialize() {
  ml_dtypes::ImportNumpy();
  import_umath1(false);

  Safe_PyObjectPtr numpy_str = make_safe(PyUnicode_FromString("numpy"));
  if (!numpy_str) {
    return false;
  }
  Safe_PyObjectPtr numpy = make_safe(PyImport_Import(numpy_str.get()));
  if (!numpy) {
    return false;
  }

  if (!RegisterFloatDtype<bfloat16>(numpy.get()) ||
      !RegisterFloatDtype<float8_e3m4>(numpy.get()) ||
      !RegisterFloatDtype<float8_e4m3>(numpy.get()) ||
      !RegisterFloatDtype<float8_e4m3b11fnuz>(numpy.get()) ||
      !RegisterFloatDtype<float8_e4m3fn>(numpy.get()) ||
      !RegisterFloatDtype<float8_e4m3fnuz>(numpy.get()) ||
      !RegisterFloatDtype<float8_e5m2>(numpy.get()) ||
      !RegisterFloatDtype<float8_e5m2fnuz>(numpy.get()) ||
      !RegisterFloatDtype<float6_e2m3fn>(numpy.get()) ||
      !RegisterFloatDtype<float6_e3m2fn>(numpy.get()) ||
      !RegisterFloatDtype<float4_e2m1fn>(numpy.get())) {
    return false;
  }
  if (!RegisterFloatDtype<float8_e8m0fnu>(numpy.get())) {
    return false;
  }

  if (!RegisterIntNDtype<int2>(numpy.get()) ||
      !RegisterIntNDtype<uint2>(numpy.get()) ||
      !RegisterIntNDtype<int4>(numpy.get()) ||
      !RegisterIntNDtype<uint4>(numpy.get())) {
    return false;
  }

  // Register casts between pairs of custom float dtypes.
  bool success =
      RegisterAllFloatCasts<bfloat16, float8_e3m4, float8_e4m3,
                            float8_e4m3b11fnuz, float8_e4m3fn, float8_e4m3fnuz,
                            float8_e5m2, float8_e5m2fnuz, float6_e2m3fn,
                            float6_e3m2fn, float4_e2m1fn>();
  // Only registering to/from BF16 and FP32 for float8_e8m0fnu.
  success &= RegisterTwoWayCustomCast<float8_e8m0fnu, bfloat16, float>();
  success &= RegisterTwoWayCustomCast<bfloat16, float8_e8m0fnu, float>();
  success &= RegisterOneWayCustomCast<int2, int4, int8_t>();
  success &= RegisterOneWayCustomCast<uint2, uint4, uint8_t>();
  return success;
}

static PyModuleDef module_def = {
    PyModuleDef_HEAD_INIT,
    "_ml_dtypes_ext",
};

// TODO(phawkins): PyMODINIT_FUNC handles visibility correctly in Python 3.9+.
// Just use PyMODINIT_FUNC after dropping Python 3.8 support.
#if defined(WIN32) || defined(_WIN32)
#define EXPORT_SYMBOL __declspec(dllexport)
#else
#define EXPORT_SYMBOL __attribute__((visibility("default")))
#endif

extern "C" EXPORT_SYMBOL PyObject* PyInit__ml_dtypes_ext() {
  Safe_PyObjectPtr m = make_safe(PyModule_Create(&module_def));
  if (!m) {
    return nullptr;
  }
  if (!Initialize()) {
    if (!PyErr_Occurred()) {
      PyErr_SetString(PyExc_RuntimeError, "cannot load _ml_dtypes_ext module.");
    }
    return nullptr;
  }

  if (!InitModuleType<float4_e2m1fn>(m.get(), "float4_e2m1fn") ||
      !InitModuleType<float6_e2m3fn>(m.get(), "float6_e2m3fn") ||
      !InitModuleType<float6_e3m2fn>(m.get(), "float6_e3m2fn") ||
      !InitModuleType<float8_e3m4>(m.get(), "float8_e3m4") ||
      !InitModuleType<float8_e4m3>(m.get(), "float8_e4m3") ||
      !InitModuleType<float8_e4m3b11fnuz>(m.get(), "float8_e4m3b11fnuz") ||
      !InitModuleType<float8_e4m3fn>(m.get(), "float8_e4m3fn") ||
      !InitModuleType<float8_e4m3fnuz>(m.get(), "float8_e4m3fnuz") ||
      !InitModuleType<float8_e5m2>(m.get(), "float8_e5m2") ||
      !InitModuleType<float8_e5m2fnuz>(m.get(), "float8_e5m2fnuz") ||
      !InitModuleType<float8_e8m0fnu>(m.get(), "float8_e8m0fnu") ||
      !InitModuleType<bfloat16>(m.get(), "bfloat16") ||
      !InitModuleType<int2>(m.get(), "int2") ||
      !InitModuleType<int4>(m.get(), "int4") ||
      !InitModuleType<uint2>(m.get(), "uint2") ||
      !InitModuleType<uint4>(m.get(), "uint4")) {
    return nullptr;
  }

#ifdef Py_GIL_DISABLED
  PyUnstable_Module_SetGIL(m.get(), Py_MOD_GIL_NOT_USED);
#endif

  return m.release();
}
}  // namespace ml_dtypes
