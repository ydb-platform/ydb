#ifndef STAN_LANG_GENERATOR_GENERATE_VAR_CONSTRUCTOR_HPP
#define STAN_LANG_GENERATOR_GENERATE_VAR_CONSTRUCTOR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_initializer.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {


    /**
     * Generate the variable constructor using the specified name,
     * type, and sizes of dimensions, rows, and columns, writing to
     * the specified stream.
     *
     * Scalar type string is `local_scalar_t__` in log_prob method,
     * `double` elsewhere.
     *
     * @param[in] var_decl variable declaration
     * @param[in] scalar_t_name name of scalar type for double values
     * @param[in,out] o stream for generating
     */

    template <typename T>
    void generate_var_constructor(const T& var_decl,
                      const std::string& scalar_t_name,
                      std::ostream& o) {
      // no constructors for scalar types - shouldn't get called
      if (var_decl.bare_type().is_primitive()) return;

      generate_bare_type(var_decl.bare_type(), scalar_t_name, o);
      generate_initializer(var_decl.type(), scalar_t_name, o);
    }
  }
}
#endif
