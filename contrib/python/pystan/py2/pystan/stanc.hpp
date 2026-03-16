#include <stan/version.hpp>
#include <stan/lang/compiler.hpp>

#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

/* REF: rstan/rstan/src/stanc.cpp */

typedef struct PyStancResult {
    int status;
    std::string msg; // for communicating errors
    std::string model_cppname;
    std::string cppcode;
    std::vector<std::string> include_paths;
} PyStancResult;


std::string stan_version() {
  std::string stan_version
    = stan::MAJOR_VERSION + "." +
      stan::MINOR_VERSION + "." +
      stan::PATCH_VERSION;
  return stan_version;
}

int stanc(std::string model_stancode, std::string model_name,
          bool allow_undefined, std::string filename,
	  std::vector<std::string> include_paths,
	  PyStancResult& result) {
  static const int SUCCESS_RC = 0;
  static const int EXCEPTION_RC = -1;
  static const int PARSE_FAIL_RC = -2;

  /*
  std::string stan_version
    = stan::MAJOR_VERSION + "." +
      stan::MINOR_VERSION + "." +
      stan::PATCH_VERSION;
  */

  std::stringstream out;
  std::istringstream in(model_stancode);
  try {
    bool valid_model
      = stan::lang::compile(&std::cerr,in,out,
                            model_name,allow_undefined,
                            filename,include_paths);
    if (!valid_model) {
      result.status = PARSE_FAIL_RC;
      return PARSE_FAIL_RC;
    }
  } catch(const std::exception& e) {
    result.status = EXCEPTION_RC;
    result.msg = e.what();
    return EXCEPTION_RC;
  }
  result.status = SUCCESS_RC;
  result.model_cppname = model_name;
  result.cppcode = out.str();
  result.include_paths = include_paths;
  return SUCCESS_RC;
}
