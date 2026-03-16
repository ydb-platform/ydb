#ifndef STAN_LANG_GENERATOR_GENERATE_ARG_DECL_HPP
#define STAN_LANG_GENERATOR_GENERATE_ARG_DECL_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the argument declaration for a function with flags
     * indicating whether to generate a <code>const</code> qualifer
     * and whether to make the variable a reference, for the specified
     * declaration and scalar type name from context, writing to the
     * specified stream.
     *
     * @param[in] gen_const true if declaration is for const
     * @param[in] gen_ref true if declaration is for reference
     * @param[in] decl argument declaration
     * @param[in] scalar_t_name string representing context scalar
     * type
     * @param[in,out] o stream for writing
     */
    void generate_arg_decl(bool gen_const, bool gen_ref, const var_decl& decl,
                           const std::string& scalar_t_name, std::ostream& o) {
      if (gen_const) o << "const ";
      generate_bare_type(decl.bare_type(), scalar_t_name, o);
      if (gen_ref) o << "&";
      o << " " << decl.name();
    }

  }
}
#endif
