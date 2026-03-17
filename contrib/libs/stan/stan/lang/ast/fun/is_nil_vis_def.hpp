#ifndef STAN_LANG_AST_FUN_IS_NIL_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_IS_NIL_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {

    bool is_nil_vis::operator()(const nil& /*x*/) const {
      return true;
    }

    bool is_nil_vis::operator()(const int_literal& /*x*/) const {
      return false;
    }

    bool is_nil_vis::operator()(const double_literal& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const array_expr& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const matrix_expr& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const row_vector_expr& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const variable& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const integrate_1d& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const fun& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const integrate_ode& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const integrate_ode_control& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const algebra_solver& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const algebra_solver_control& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const map_rect& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const index_op& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const index_op_sliced& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const conditional_op& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const binary_op& /* x */) const {
      return false;
    }

    bool is_nil_vis::operator()(const unary_op& /* x */) const {
      return false;
    }

  }
}
#endif
