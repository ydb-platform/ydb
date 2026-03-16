#ifndef STAN_LANG_AST_FUN_PRINT_SCOPE_DEF_HPP
#define STAN_LANG_AST_FUN_PRINT_SCOPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    void print_scope(std::ostream& o, const scope& var_scope) {
      if (var_scope.program_block() == model_name_origin)
        o << "model name";
      else if (var_scope.program_block() == data_origin)
        o << "data";
      else if (var_scope.program_block() == transformed_data_origin)
        o << "transformed data";
      else if (var_scope.program_block() == parameter_origin)
        o << "parameter";
      else if (var_scope.program_block() == transformed_parameter_origin)
        o << "transformed parameter";
      else if (var_scope.program_block() == derived_origin)
        o << "generated quantities";
      else if (var_scope.program_block() == function_argument_origin)
        o << "function argument";
      else if (var_scope.program_block() == function_argument_origin_lp)
        o << "function argument '_lp' suffixed";
      else if (var_scope.program_block() == function_argument_origin_rng)
        o << "function argument '_rng' suffixed";
      else if (var_scope.program_block() == void_function_argument_origin)
        o << "void function argument";
      else if (var_scope.program_block() == void_function_argument_origin_lp)
        o << "void function argument '_lp' suffixed";
      else if (var_scope.program_block() == void_function_argument_origin_rng)
        o << "void function argument '_rng' suffixed";
      else if (var_scope.program_block() == loop_identifier_origin)
        o << "loop identifier";
      else
        o << "UNKNOWN ORIGIN=" << var_scope.program_block();
    }

  }
}
#endif
