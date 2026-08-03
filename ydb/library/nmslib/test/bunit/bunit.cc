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
#include <stdlib.h>

#include <string>

#include "bunit.h"
#include "init.h"

namespace similarity {

TestRunner& TestRunner::Instance() {
  static TestRunner instance;
  return instance;
}

void TestRunner::AddTest(const std::string& test_name, TestBase* test_instance) {
  const size_t pos = test_name.find(kDisable);
  bool disabled = false;
  if (pos != std::string::npos) {
    if (pos == 0) {
      disabled = true;
    } else {
      std::cout << "Incorrect test name " << test_name << std::endl;
      exit(1);
    }
  }
  tests_.push_back(std::make_tuple(test_name, test_instance, disabled));
}

int TestRunner::RunAllTests() {
  int num_failed = 0;
  int num_disabled = 0;
  for (auto it = tests_.begin(); it != tests_.end(); ++it) {
    std::cout << "----- " << std::get<0>(*it) << " -----" << std::endl;
    if (std::get<2>(*it)) {
      ++num_disabled;
      std::cout << yellow << "disabled" << no_color << std::endl;
    } else {
      try {
        std::get<1>(*it)->Test();
        std::cout << green << "passed" << no_color << std::endl;
      } catch (const TestException& ex) {
          ++num_failed;
          std::cout << red << "failed" << no_color << std::endl;
          std::cout << ex.what() << std::endl;
      } catch (const std::exception& ex) {
        ++num_failed;
        std::cout << red << "failed" << no_color << std::endl;
        std::cout << ex.what() << std::endl;
      } catch (...) {
        std::cout << red << "failed" << no_color << std::endl;
        std::cout << "unknown cause" << std::endl;
      }
    }
  }

  std::cout << "======================================" << std::endl;
  std::cout << green << "In total " << tests_.size() << " testcases";
  if (num_disabled > 0) {
    std::cout << " (" << num_disabled << " tests disabled)";
  }
  std::cout << no_color << std::endl;

  if (num_failed == 0) {
    std::cout << green << "ALL TESTS PASSED" << no_color << std::endl;
  } else {
    std::cout << red << "FAILED " << num_failed
              << " TESTS !!!" << no_color << std::endl;
  }
  std::cout << "======================================" << std::endl;

  return num_failed != 0;
}

}     // namespace similarity

int main(int argc, char *argv[]) {
  std::string LogFile;
  if (argc == 2) LogFile = argv[1];
  similarity::initLibrary(0, LogFile.empty() ? LIB_LOGSTDERR:LIB_LOGFILE, LogFile.c_str());

  return similarity::TestRunner::Instance().RunAllTests();
}
