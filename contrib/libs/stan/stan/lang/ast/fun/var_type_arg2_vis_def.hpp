#ifndef STAN_LANG_AST_FUN_VAR_TYPE_ARG2_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_VAR_TYPE_ARG2_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {
    var_type_arg2_vis::var_type_arg2_vis() { }

    expression
    var_type_arg2_vis::operator()(const block_array_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const local_array_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const cholesky_factor_corr_block_type& x)
      const {
      return x.K_;
    }

    expression
    var_type_arg2_vis::operator()(const cholesky_factor_cov_block_type& x)
      const {
      return x.N_;
    }

    expression
    var_type_arg2_vis::operator()(const corr_matrix_block_type& x)
      const {
      return x.K_;
    }

    expression
    var_type_arg2_vis::operator()(const cov_matrix_block_type& x)
      const {
      return x.K_;
    }

    expression
    var_type_arg2_vis::operator()(const double_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const double_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const ill_formed_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const int_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const int_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const matrix_block_type& x)
      const {
      return x.N_;
    }

    expression
    var_type_arg2_vis::operator()(const matrix_local_type& x)
      const {
      return x.N_;
    }

    expression
    var_type_arg2_vis::operator()(const ordered_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const positive_ordered_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const row_vector_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const row_vector_local_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const simplex_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const unit_vector_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const vector_block_type& x)
      const {
      return expression(nil());
    }

    expression
    var_type_arg2_vis::operator()(const vector_local_type& x)
      const {
      return expression(nil());
    }
  }
}
#endif
