#ifndef STAN_LANG_GENERATOR_GENERATE_MEMBER_VAR_DECLS_ALL_HPP
#define STAN_LANG_GENERATOR_GENERATE_MEMBER_VAR_DECLS_ALL_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_member_var_decls.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate member variable declarations for the data and
     * transformed data blocks for the specified program, writing to
     * the specified stream.
     *
     * @param[in] prog program from which to generate
     * @param[in,out] o stream for generating
     */
    void generate_member_var_decls_all(const program& prog,
                                       std::ostream& o) {
      generate_member_var_decls(prog.data_decl_, 1, o);
      generate_member_var_decls(prog.derived_data_decl_.first, 1, o);
    }

  }
}
#endif
