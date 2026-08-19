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
#include "logging.h"
#include "utils.h"
#include "bunit.h"

#include <limits>

using namespace std;

namespace similarity {

template <class dist_t>
void testEqualInt(dist_t num1, dist_t num2) {
  EXPECT_TRUE(ApproxEqual(num1, num2, 4));
}

template <class dist_t>
void testNotEqualInt(dist_t num1, dist_t num2) {
  EXPECT_FALSE(ApproxEqual(num1, num2, 4));
}

template <class dist_t>
void testEqualFP(dist_t baseNum, float epsFact) {
  EXPECT_TRUE(ApproxEqual(baseNum, baseNum * (1 + epsFact * numeric_limits<dist_t>::epsilon()), 4));
  EXPECT_TRUE(ApproxEqual(-baseNum, -baseNum * (1 + epsFact * numeric_limits<dist_t>::epsilon()), 4));
}

template <class dist_t>
void testNotEqualFP(dist_t baseNum, float epsFact) {
  EXPECT_FALSE(ApproxEqual(baseNum, baseNum * (1 + epsFact * numeric_limits<dist_t>::epsilon()), 4));
  EXPECT_FALSE(ApproxEqual(-baseNum, -baseNum * (1 + epsFact * numeric_limits<dist_t>::epsilon()), 4));
}

TEST(FP_Char1) {
  testEqualInt<char>(1, 1);
}

TEST(FP_Char2) {
  testEqualInt<char>(125, 125);
}

TEST(FP_Char3) {
  testNotEqualInt<char>(1, 2);
}

TEST(FP_Char4) {
  testNotEqualInt<char>(0, -1);
}

TEST(FP_Short1) {
  testEqualInt<short>(1, 1);
}

TEST(FP_Short2) {
  testEqualInt<short>(32767, 32767);
}

TEST(FP_Short3) {
  testNotEqualInt<short>(1, 2);
}

TEST(FP_Short4) {
  testNotEqualInt<short>(0, -1);
}

TEST(FP_Int1) {
  testEqualInt<int>(1, 1);
}

TEST(FP_Int2) {
  testEqualInt<int>(65535, 65535);
}

TEST(FP_Int3) {
  testNotEqualInt<int>(1, 2);
}

TEST(FP_Int4) {
  testNotEqualInt<int>(0, -1);
}

TEST(FP_Unsigned1) {
  testEqualInt<unsigned>(1, 1);
}

TEST(FP_Unsigned2) {
  testEqualInt<unsigned>(65535, 65535);
}

TEST(FP_Unsigned3) {
  testNotEqualInt<unsigned>(1, 2);
}

TEST(FP_Float1) {
  testEqualFP<float>(1.0, 1);
  testEqualFP<float>(1.0, 2);
  testEqualFP<float>(1.0, 3);
  testEqualFP<float>(1.0, 4);
}

TEST(FP_Float2) {
  testEqualFP<float>(1e4f, 1);
  testEqualFP<float>(1e4f, 2);
  testEqualFP<float>(1e4f, 3);
}

TEST(FP_Float3) {
  testEqualFP<float>(1e-5f, 1);
  testEqualFP<float>(1e-5f, 2);
  testEqualFP<float>(1e-5f, 3);
}

TEST(FP_Float4) {
  testNotEqualFP<float>(1.0, 5);
}

TEST(FP_Float5) {
  testNotEqualFP<float>(0.5, 5);
}

TEST(FP_Double1) {
  testEqualFP<double>(1.0, 1);
  testEqualFP<double>(1.0, 2);
  testEqualFP<double>(1.0, 3);
  testEqualFP<double>(1.0, 4);
}

TEST(FP_Double2) {
  testEqualFP<double>(1e7, 1);
  testEqualFP<double>(1e7, 2);
  testEqualFP<double>(1e7, 3);
}

TEST(FP_Double3) {
  testEqualFP<double>(1e-6, 1);
  testEqualFP<double>(1e-6, 2);
  testEqualFP<double>(1e-6, 3);
}

TEST(FP_Double4) {
  testNotEqualFP<double>(1.0, 5);
}

TEST(FP_Double5) {
  testNotEqualFP<double>(0.5, 5);
}

TEST(FP_LongDouble1) {
  testEqualFP<long double>(1.0, 1);
  testEqualFP<long double>(1.0, 2);
  testEqualFP<long double>(1.0, 3);
}

TEST(FP_LongDouble2) {
  testEqualFP<long double>(2, 1);
  testEqualFP<long double>(2, 2);
  testEqualFP<long double>(2, 3);
}

TEST(FP_LongDouble3) {
  testNotEqualFP<long double>(1.0, 5);
  testNotEqualFP<long double>(10.0, 5);
  testNotEqualFP<long double>(10e10, 5);
}

TEST(FP_LongDouble4) {
  testNotEqualFP<long double>(0.5, 5);
  testNotEqualFP<long double>(0.05, 5);
  testNotEqualFP<long double>(1e-10, 5);
}

TEST(FP_NANFloat) {
  static float __nan = numeric_limits<float>::quiet_NaN();
  testNotEqualFP<float>(__nan, __nan);
  testNotEqualFP<float>(__nan, -__nan);
};

TEST(FP_NANDouble) {
  static double __nan = numeric_limits<double>::quiet_NaN();
  testNotEqualFP<double>(__nan, __nan);
  testNotEqualFP<double>(__nan, -__nan);
};

TEST(FP_NANLongDouble) {
  static long double __nan = numeric_limits<long double>::quiet_NaN();
  testNotEqualFP<long double>(__nan, __nan);
  testNotEqualFP<long double>(__nan, -__nan);
};

TEST(FP_ZEROFloat) {
  EXPECT_TRUE(ApproxEqual(0.0f, -0.0f));
  EXPECT_TRUE(ApproxEqual(0.0f, numeric_limits<float>::min()));
  EXPECT_TRUE(ApproxEqual(0.0f, 1.9f*numeric_limits<float>::min()));
  EXPECT_FALSE(ApproxEqual(0.0f, 5000000.0f*numeric_limits<float>::min()));
  EXPECT_TRUE(ApproxEqual(0.0f, -numeric_limits<float>::min()));
  EXPECT_TRUE(ApproxEqual(0.0f, -1.9f*numeric_limits<float>::min()));
  EXPECT_FALSE(ApproxEqual(0.0f, -5000000.0f*numeric_limits<float>::min()));
}

TEST(FP_ZERODouble) {
  EXPECT_TRUE(ApproxEqual(0.0, -0.0));
  EXPECT_TRUE(ApproxEqual(0.0, numeric_limits<double>::min()));
  EXPECT_TRUE(ApproxEqual(0.0, 1.9*numeric_limits<double>::min()));
  EXPECT_FALSE(ApproxEqual(0.0, 2.1*numeric_limits<double>::min()));
  EXPECT_TRUE(ApproxEqual(0.0, -numeric_limits<double>::min()));
  EXPECT_TRUE(ApproxEqual(0.0, -1.9*numeric_limits<double>::min()));
  EXPECT_FALSE(ApproxEqual(0.0, -2.1*numeric_limits<double>::min()));
}

TEST(FP_ZEROLongDouble) {
  EXPECT_TRUE(ApproxEqual(0.0l, -0.0l));
  EXPECT_TRUE(ApproxEqual(0.0l, numeric_limits<long double>::min()));
  EXPECT_TRUE(ApproxEqual(0.0l, 1.9*numeric_limits<long double>::min()));
  EXPECT_FALSE(ApproxEqual(0.0l, 100.1*numeric_limits<long double>::min()));
  EXPECT_TRUE(ApproxEqual(0.0l, -numeric_limits<long double>::min()));
  EXPECT_TRUE(ApproxEqual(0.0l, -1.9*numeric_limits<long double>::min()));
  EXPECT_FALSE(ApproxEqual(0.0l, -10.1*numeric_limits<long double>::min()));
}

}
