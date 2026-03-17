#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATION_NAME_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATION_NAME_HPP

#include <stan/lang/ast.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate a name for a non-variable (double only) instantiation of
     * specified function

     * @param[in] fun function AST object
     * @param[in, out] out output stream to which function definition
     * is written
     */
    void generate_function_instantiation_name(const function_decl_def& fun,
                           std::ostream& out) {
      out << fun.name_;
    }

  }
}
#endif
