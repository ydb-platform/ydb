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

#ifndef _BUNIT_H_
#define _BUNIT_H_

#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <functional>
#include <utility>
#include <tuple>
#include <cmath>
#include <exception>

// Uncomment this to disable tests taking long time
//#define DISABLE_LONG_TESTS

namespace similarity {

using TestFunc = std::function<void()>;

class TestException : public std::exception {
 public:
  TestException(const std::string& msg) : msg_(msg) {}
  const char* what() const 
#ifndef _MSC_VER
// TODO: @leo/bileg Do we really need noexcept here, MSVS chokes on it
	  noexcept 
#endif
  { return msg_.c_str(); }
 private:
  const std::string msg_;
};

const std::string kDisable = "DISABLE_";

class TestBase {
public:
  virtual void Test() = 0;
  virtual ~TestBase(){}
};

class TestRunner {
 public:
  ~TestRunner() {}

  static TestRunner& Instance();
  void AddTest(const std::string& test_name, TestBase* test_instance);
  int RunAllTests();

 private:
  TestRunner() {}
  // <test_name, test_instance, is_disabled>
  std::vector<std::tuple<std::string,TestBase*,bool>> tests_;
};

#define TEST(test_name) \
  class _Test_##test_name##_ : TestBase { \
   public: \
    _Test_##test_name##_() { \
      similarity::TestRunner::Instance().AddTest(#test_name, this); \
    } \
    virtual ~_Test_##test_name##_() {} \
   private: \
    virtual void Test(); \
  }; \
  similarity::_Test_##test_name##_ _test_##test_name##_; \
  void _Test_##test_name##_::Test()


template <typename T>
inline bool EQ(const T& x, const T& y, T eps) {
  return x == y;
}

template <>
inline bool EQ<float>(const float& x, const float& y, float eps) {
  // In C++ 11, std::abs is also defined for real-valued arguments
  return std::abs(x - y) <= eps;
}

template <>
inline bool EQ<double>(const double& x, const double& y, double eps) {
  // In C++ 11, std::abs is also defined for real-valued arguments
  return std::abs(x - y) <= eps;
}

template <typename T>
static inline void Expect_EQ(const std::string& msg,
                             const T& expected,
                             const T& actual,
                             const std::string& file_name,
                             int line_num,
                             T eps = static_cast<T>(1e-10)) {
  if (!EQ(expected, actual, eps)) {
    std::stringstream ss;
    ss << file_name << "(" << line_num << "): "
       << "EXPECT_EQ(" << msg << ") " << std::endl
       << "   expected: " << expected << std::endl
       << "   actual  : " << actual
       << std::endl;
    throw TestException(ss.str());
  }
}

template <typename T>
static inline void Expect_NE(const std::string& msg,
                             const T& expected,
                             const T& actual,
                             const std::string& file_name,
                             int line_num,
                             T eps = static_cast<T>(1e-10)) {
  if (EQ(expected, actual, eps)) {
    std::stringstream ss;
    ss << file_name << "(" << line_num << "): "
       << "EXPECT_NE(" << msg << ") " << std::endl
       << "   expected: " << expected << std::endl
       << "   actual  : " << actual
       << std::endl;
    throw TestException(ss.str());
  }
}

static inline void Expect_True(const std::string& msg,
                               const int condition,
                               const std::string& file_name,
                               int line_num) {
  if (!condition) {
    std::stringstream ss;
    ss << file_name << "(" << line_num << "): "
       << "EXPECT_TRUE(" << msg << ") " << std::endl;
    throw TestException(ss.str());
  }
}

static inline void Expect_False(const std::string& msg,
                                const int condition,
                                const std::string& file_name,
                                int line_num) {
  if (condition) {
    std::stringstream ss;
    ss << file_name << "(" << line_num << "): "
       << " EXPECT_FALSE(" << msg << ") " << std::endl;
    throw TestException(ss.str());
  }
}

#define EXPECT_EQ(expected, actual) \
  similarity::Expect_EQ(#expected ", " #actual, (expected), (actual), __FILE__, __LINE__)

#define EXPECT_EQ_EPS(expected, actual, eps) \
  similarity::Expect_EQ(#expected ", " #actual, (expected), (actual), __FILE__, __LINE__, eps)

#define EXPECT_NE(expected, actual) \
  similarity::Expect_NE(#expected ", " #actual, (expected), (actual), __FILE__, __LINE__)

#define EXPECT_NE_EPS(expected, actual, eps) \
  similarity::Expect_NE(#expected ", " #actual, (expected), (actual), __FILE__, __LINE__, eps)

#define EXPECT_TRUE(condition) \
  similarity::Expect_True(#condition, (condition), __FILE__, __LINE__)

#define EXPECT_FALSE(condition) \
  similarity::Expect_False(#condition, (condition), __FILE__, __LINE__)

#ifdef __linux
const std::string green = "\x1b[32m";
const std::string red = "\x1b[31m";
const std::string yellow = "\x1b[33m";
const std::string no_color = "\x1b[0m";
#else
const std::string green;
const std::string red;
const std::string yellow;
const std::string no_color;
#endif

}     // namespace similarity

#endif    // _BUNIT_H_
