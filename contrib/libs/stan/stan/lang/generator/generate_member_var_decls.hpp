#ifndef STAN_LANG_GENERATOR_GENERATE_MEMBER_VAR_DECLS_HPP
#define STAN_LANG_GENERATOR_GENERATE_MEMBER_VAR_DECLS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <stan/lang/generator/get_typedef_var_type.hpp>
#include <ostream>
#include <vector>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate model class member variable declarations for
     * variables declared in data and transformed data blocks
     * at the specified indentation level to the specified stream.
     *
     * NOTE: variable decls typedefs defined in stan::math.
     *
     * @param[in] vs variable declarations
     * @param[in] indent indentation level
     * @param[in] o stream for writing
     */
    void generate_member_var_decls(const std::vector<block_var_decl>& vs,
                                   int indent, std::ostream& o) {
      for (size_t i = 0; i < vs.size(); ++i) {
        generate_indent(indent, o);
        std::string var_name(vs[i].name());
        block_var_type btype = vs[i].type().innermost_type();
        std::string typedef_var_type = get_typedef_var_type(btype.bare_type());
        int ar_dims = vs[i].type().array_dims();

        for (int i = 0; i < indent; ++i)
          o << INDENT;
        for (int i = 0; i < ar_dims; ++i)
          o << "std::vector<";
        o << typedef_var_type;
        if (ar_dims > 0)
          o << ">";
        for (int i = 1; i < ar_dims; ++i)
          o << " >";
        o << " " << var_name << ";" << EOL;
      }
    }
  }
}
#endif
