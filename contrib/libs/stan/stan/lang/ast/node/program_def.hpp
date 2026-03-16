#ifndef STAN_LANG_AST_NODE_PROGRAM_DEF_HPP
#define STAN_LANG_AST_NODE_PROGRAM_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    program::program() { }

    program::program(const function_decls_t& functions,
                     const block_var_decls_t& data,
                     const var_decls_statements_t& transformed_data,
                     const block_var_decls_t& parameters,
                     const var_decls_statements_t& transformed_parameters,
                     const statement& model,
                     const var_decls_statements_t& generated_quantities)
      : function_decl_defs_(functions), data_decl_(data),
        derived_data_decl_(transformed_data), parameter_decl_(parameters),
        derived_decl_(transformed_parameters), statement_(model),
        generated_decl_(generated_quantities) { }

  }
}
#endif
