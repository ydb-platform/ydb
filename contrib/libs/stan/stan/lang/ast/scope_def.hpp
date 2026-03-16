#ifndef STAN_LANG_AST_SCOPE_DEF_HPP
#define STAN_LANG_AST_SCOPE_DEF_HPP

#include <stan/lang/ast/origin_block.hpp>
#include <stan/lang/ast/scope.hpp>

namespace stan {
  namespace lang {

    scope::scope() : program_block_(model_name_origin), is_local_(false) { }

    scope::scope(const origin_block& program_block)
      : program_block_(program_block), is_local_(false) { }

    scope::scope(const origin_block& program_block,
                 const bool& is_local)
      : program_block_(program_block), is_local_(is_local) { }


    origin_block scope::program_block() const {
      return program_block_;
    }

    bool scope::is_local() const {
      return is_local_;
    }

    bool scope::local_allows_var() const {
      return is_local_
        && program_block_ != transformed_data_origin
        && program_block_ != derived_origin;
    }

    bool scope::par_or_tpar() const {
      return !is_local_
        && (program_block_ == parameter_origin
            || program_block_ == transformed_parameter_origin);
    }

    bool scope::tpar() const {
      return  program_block_ == transformed_parameter_origin;
    }

    bool scope::fun() const {
      return program_block_ == function_argument_origin
        || program_block_ == function_argument_origin_lp
        || program_block_ == function_argument_origin_rng
        || program_block_ == void_function_argument_origin
        || program_block_ == void_function_argument_origin_lp
        || program_block_ == void_function_argument_origin_rng;
    }

    bool scope::non_void_fun() const {
      return program_block_ == function_argument_origin
        || program_block_ == function_argument_origin_lp
        || program_block_ == function_argument_origin_rng;
    }

    bool scope::void_fun() const {
      return program_block_ == void_function_argument_origin
        || program_block_ == void_function_argument_origin_lp
        || program_block_ == void_function_argument_origin_rng;
    }

    bool scope::allows_assignment() const {
      return !(program_block_ == data_origin
               || program_block_ == parameter_origin);
    }

    bool scope::allows_lp_fun() const {
      return program_block_ == model_name_origin
        || program_block_ == transformed_parameter_origin
        || program_block_ == function_argument_origin_lp
        || program_block_ == void_function_argument_origin_lp;
    }

    bool scope::allows_rng() const {
      return program_block_ == derived_origin
        || program_block_ == transformed_data_origin
        || program_block_ == function_argument_origin_rng
        || program_block_ == void_function_argument_origin_rng;
    }

    bool scope::allows_sampling() const {
      return program_block_ == model_name_origin
        || program_block_ == function_argument_origin_lp
        || program_block_ == void_function_argument_origin_lp;
    }

    bool scope::allows_size() const {
      return is_local_
        || program_block_ == data_origin
        || program_block_ == transformed_data_origin
        || program_block_ == function_argument_origin
        || program_block_ == function_argument_origin_lp
        || program_block_ == function_argument_origin_rng
        || program_block_ == void_function_argument_origin
        || program_block_ == void_function_argument_origin_lp
        || program_block_ == void_function_argument_origin_rng;
    }
  }
}
#endif
