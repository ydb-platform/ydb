/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#include <cmath>
#include <cstring>
#include <cstdint>


#ifdef _MSC_VER
#include <time.h>
#include <io.h>
#ifndef F_OK
#define F_OK 0
#include <direct.h>       // for mkdir
#define mkdir(name, mode) mkdir(name)
#endif
#else
#include <unistd.h>
#include <sys/time.h>
#include <sys/stat.h>     // for mkdir
#endif

#include "global.h"       // for snprintf
#include "utils.h"
#include "logging.h"
#include "floatdiff.h"

namespace similarity {

const char* GetFileName(const char* fullpath) {
  for (int i = strlen(fullpath) - 1; i >= 0; --i) {
    if (fullpath[i] == '\\' || fullpath[i] == '/') {
      return fullpath + i + 1;
    }
  }
  return fullpath;
}

bool DoesFileExist(const char *filename) {
#ifdef _MSC_VER
  return _access(filename, F_OK) == 0;
#else
  return access(filename, F_OK) == 0;
#endif
}

void RStrip(char* str) {
  int i = strlen(str) - 1;
  while ((i >= 0) &&
         (str[i] == '\r' || str[i] == '\n' || str[i] == ' ' || str[i] == '\t'))
    str[i--] = '\0';
}

// This macro does two things: (a) specialization (b) instantiation
#define DECLARE_APPROX_EQUAL_INT(INT_TYPE) \
template <> bool ApproxEqual<INT_TYPE>(const INT_TYPE& x, const INT_TYPE& y, unsigned) { return x == y; }\
template bool ApproxEqual<INT_TYPE>(const INT_TYPE& x, const INT_TYPE& y, unsigned);

DECLARE_APPROX_EQUAL_INT(uint64_t)
DECLARE_APPROX_EQUAL_INT(int64_t)
DECLARE_APPROX_EQUAL_INT(uint32_t)
DECLARE_APPROX_EQUAL_INT(int32_t)
DECLARE_APPROX_EQUAL_INT(uint16_t)
DECLARE_APPROX_EQUAL_INT(int16_t)
DECLARE_APPROX_EQUAL_INT(uint8_t)
DECLARE_APPROX_EQUAL_INT(int8_t)
DECLARE_APPROX_EQUAL_INT(char)

template <typename T>
bool ApproxEqualULP(const T& x, const T& y, unsigned maxUlps = MAX_ULPS);

// This macro does two things: (a) specialization (b) instantiation
#define DECLARE_APPROX_EQUAL_ULP_FLOAT(FLOAT_TYPE) \
template <> bool ApproxEqualULP<FLOAT_TYPE>(const FLOAT_TYPE& x, const FLOAT_TYPE& y, unsigned maxUlps) {\
  return FloatingPointDiff<FLOAT_TYPE>(x).AlmostEquals(FloatingPointDiff<FLOAT_TYPE>(y), maxUlps);\
};\
template bool ApproxEqualULP<FLOAT_TYPE>(const FLOAT_TYPE& x, const FLOAT_TYPE& y, unsigned maxUlps);

DECLARE_APPROX_EQUAL_ULP_FLOAT(float)
DECLARE_APPROX_EQUAL_ULP_FLOAT(double)

// This macro does two things: (a) specialization (b) instantiation
#define DECLARE_APPROX_EQUAL_FLOAT(FLOAT_TYPE) \
template <> bool ApproxEqual<FLOAT_TYPE>(const FLOAT_TYPE& x, const FLOAT_TYPE& y, unsigned maxUlps) {\
  const FLOAT_TYPE thresh = 2*numeric_limits<FLOAT_TYPE>::min();\
  return ApproxEqualULP<FLOAT_TYPE>(x, y, maxUlps) || (std::max(x,y) < thresh && std::min(x,y) > -thresh);\
};\
template bool ApproxEqual<FLOAT_TYPE>(const FLOAT_TYPE& x, const FLOAT_TYPE& y, unsigned maxUlps);

DECLARE_APPROX_EQUAL_FLOAT(float)
DECLARE_APPROX_EQUAL_FLOAT(double)

/*
 * This is just an approximate number of ULPs
 * Can't do better, because FloatingPointDiff doesn't support long double.
 */
template <typename T>
inline bool ApproxEqualOther(const T& x, const T& y, unsigned maxUlps) {
  // In C++ 11, std::abs is also defined for floating-point numbers
  return std::abs(x - y) <= 0.75*maxUlps*std::numeric_limits<T>::epsilon()*
                            std::min(std::abs(x), std::abs(y));
}

template <> bool ApproxEqual<long double>(const long double& x, const long double& y, unsigned maxUlps) {
  const long double thresh = 2*numeric_limits<long double>::min();\
  return ApproxEqualOther(x, y, maxUlps) || (std::max(x,y) < thresh && std::min(x,y) > -thresh);\
};\
template bool ApproxEqual<long double>(const long double& x, const long double& y, unsigned maxUlps);

// Not possible to use FloatingPointDiff for long double

}  // namespace similarity

