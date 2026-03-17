#ifndef STAN_LANG_GENERATOR_WRITE_VAR_DECL_TYPE_HPP
#define STAN_LANG_GENERATOR_WRITE_VAR_DECL_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <string>
#include <ostream>

namespace stan {
  namespace lang {
    /**
     * Write the variable type declaration to the specified stream
     * using the specified cpp type string, which varies according
     * where in the generated model class this decl occurs.
     * Currently, member var decls and ctor use typdefs,
     * other methods have explicit types with typedef on
     * scalar double types.
     *
     * Note:  this is called after array type has been unfolded,
     * so bare_type shouldn't be bare_array_type (or ill_formed_type).
     *
     * @param[in] bare_type variable type
     * @param[in] cpp_type_str generated cpp type
     * @param[in] ar_dims of array dimensions
     * @param[in] indent indentation level
     * @param[in,out] o stream for generating
     */
    void
    write_var_decl_type(const bare_expr_type& bare_type,
                        const std::string& cpp_type_str,
                        int ar_dims,
                        int indent,
                        std::ostream& o) {
      bool ends_with_angle
        = cpp_type_str.at(cpp_type_str.length()-1) == '>';

      for (int i = 0; i < indent; ++i)
        o << INDENT;
      for (int i = 0; i < ar_dims; ++i)
        o << "std::vector<";
      o << cpp_type_str;
      for (int i = 0; i < ar_dims; ++i) {
        if (ar_dims > 0 || ends_with_angle)
          o << " ";  // maybe not needed for c++11
        o << " >";
      }
    }
  }
}
#endif
