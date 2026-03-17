#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_NAME_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_NAME_HPP

#include <stan/lang/ast.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the function name from the specified declaration on
     * the specified stream.
     *
     * @param[in] fun function declaration
     * @param[in,out] o stream for generating
     */
    void generate_function_name(const function_decl_def& fun,
                                std::ostream& o) {
      o << fun.name_;
    }

  }
}
#endif
