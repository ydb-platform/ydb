#ifndef STAN_LANG_GENERATOR_FUN_SCALAR_TYPE_HPP
#define STAN_LANG_GENERATOR_FUN_SCALAR_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <ostream>
#include <sstream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Return the string representing the scalar type to use in the
     * body of the specified function declaration, with a flag
     * indicating if the function is a log probability function.
     *
     * @param[in] fun function declaration
     * @param[in] is_lp true if the function is a log probability
     * function
     * @return string representing scalar type to use within the
     * function body
     */
    std::string fun_scalar_type(const function_decl_def& fun, bool is_lp) {
      size_t num_args = fun.arg_decls_.size();
      // nullary, non-lp
      if (fun.has_only_int_args() && !is_lp)
        return "double";

      // need template metaprogram to construct return
      std::stringstream ss;
      ss << "typename boost::math::tools::promote_args<";
      int num_open_brackets = 1;
      int num_generated_params = 0;
      for (size_t i = 0; i < num_args; ++i) {
        if (!fun.arg_decls_[i].bare_type().innermost_type().is_int_type()) {
          if (num_generated_params > 0)
            ss << ", ";
          // break into blocks of 4 and apply promotion recursively
          // setting at 4 leaves room for an extra parameter at the end
          if (num_generated_params == 4) {
            ss << "typename boost::math::tools::promote_args<";
            num_generated_params = 0;
            ++num_open_brackets;
          }
          ss << "T" << i << "__";
          ++num_generated_params;
        }
      }
      if (is_lp) {
        if (num_generated_params > 0)
          ss << ", ";
        ss << "T_lp__";
      }
      for (int i = 0; i < num_open_brackets; ++i)
        ss << ">::type";
      return ss.str();
    }

  }
}
#endif
