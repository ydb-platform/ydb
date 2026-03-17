#ifndef STAN_LANG_GENERATOR_GENERATE_VALIDATE_VAR_DIMS_HPP
#define STAN_LANG_GENERATOR_GENERATE_VALIDATE_VAR_DIMS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_validate_nonnegative.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate code to validate variable sizes for array dimensions
     * and vector/matrix number of rows, columns to the specified stream
     * at the specified level of indentation.
     *
     * @param[in] var_decl variable declaration
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    template <typename T>
    void generate_validate_var_dims(const T& var_decl,
                                    int indent, std::ostream& o) {
      std::string name(var_decl.name());
      expression arg1 = var_decl.type().innermost_type().arg1();
      expression arg2 = var_decl.type().innermost_type().arg2();
      std::vector<expression> ar_var_dims = var_decl.type().array_lens();

      if (!is_nil(arg1))
        generate_validate_nonnegative(name, arg1, indent, o);

      if (!is_nil(arg2))
        generate_validate_nonnegative(name, arg2, indent, o);

      for (size_t i = 0; i < ar_var_dims.size(); ++i)
        generate_validate_nonnegative(name, ar_var_dims[i], indent, o);
    }


    template void generate_validate_var_dims(const block_var_decl& var_decl,
                                             int indent, std::ostream& o);

    template void generate_validate_var_dims(const local_var_decl& var_decl,
                                             int indent, std::ostream& o);
  }
}
#endif
