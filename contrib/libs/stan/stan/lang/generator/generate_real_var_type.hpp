#ifndef STAN_LANG_GENERATOR_GENERATE_REAL_VAR_TYPE_HPP
#define STAN_LANG_GENERATOR_GENERATE_REAL_VAR_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <ostream>

namespace stan {
  namespace lang {

   /**
     * Generate correct C++ type for expressions which contain a Stan
     * <code>real</code> variable according to scope in which
     * expression is used and expression contents.
     *
     * @param[in] var_scope expression origin block
     * @param[in] has_var does expression contains a variable?
     * @param[in,out] o generated typename
     */
    void generate_real_var_type(const scope& var_scope,
                                bool has_var,
                                std::ostream& o) {
      if (var_scope.fun() || has_var)
        o << "local_scalar_t__";
      else
        o << "double";
    }

  }
}
#endif
