#ifndef STAN_LANG_AST_FUN_VAR_OCCURS_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_VAR_OCCURS_VIS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <boost/variant/apply_visitor.hpp>

namespace stan {
  namespace lang {

    var_occurs_vis::var_occurs_vis(const variable& e)
      : var_name_(e.name_) {
    }

    bool var_occurs_vis::operator()(const nil& st) const {
      return false;
    }

    bool var_occurs_vis::operator()(const int_literal& e) const {
      return false;
    }

    bool var_occurs_vis::operator()(const double_literal& e) const {
      return false;
    }

    bool var_occurs_vis::operator()(const array_expr& e) const {
      for (size_t i = 0; i < e.args_.size(); ++i)
        if (boost::apply_visitor(*this, e.args_[i].expr_))
          return true;
      return false;
    }

    bool var_occurs_vis::operator()(const matrix_expr& e) const {
      for (size_t i = 0; i < e.args_.size(); ++i)
        if (boost::apply_visitor(*this, e.args_[i].expr_))
          return true;
      return false;
    }

    bool var_occurs_vis::operator()(const row_vector_expr& e) const {
      for (size_t i = 0; i < e.args_.size(); ++i)
        if (boost::apply_visitor(*this, e.args_[i].expr_))
          return true;
      return false;
    }

    bool var_occurs_vis::operator()(const variable& e) const {
      return var_name_ == e.name_;
    }

    bool var_occurs_vis::operator()(const fun& e) const {
      for (size_t i = 0; i < e.args_.size(); ++i)
        if (boost::apply_visitor(*this, e.args_[i].expr_))
          return true;
      return false;
    }

    bool var_occurs_vis::operator()(const integrate_1d& e) const {
      return false;  // no refs persist out of integrate_1d() call
    }

    bool var_occurs_vis::operator()(const integrate_ode& e) const {
      return false;  // no refs persist out of integrate_ode() call
    }

    bool var_occurs_vis::operator()(const integrate_ode_control& e) const {
      return false;  // no refs persist out of integrate_ode_control() call
    }

    bool var_occurs_vis::operator()(const algebra_solver& e) const {
      return false;  // no refs persist out of algebra_solver() call
    }

    bool var_occurs_vis::operator()(const algebra_solver_control& e) const {
      return false;  // no refs persist out of algebra_solver_control() call
    }

    bool var_occurs_vis::operator()(const map_rect& e) const {
      return false;  // no refs persist out of map_rect() call
    }

    bool var_occurs_vis::operator()(const index_op& e) const {
      // refs only persist out of expression, not indexes
      return boost::apply_visitor(*this, e.expr_.expr_);
    }

    bool var_occurs_vis::operator()(const index_op_sliced& e) const {
      return boost::apply_visitor(*this, e.expr_.expr_);
    }

    bool var_occurs_vis::operator()(const conditional_op& e) const {
      return boost::apply_visitor(*this, e.cond_.expr_)
        || boost::apply_visitor(*this, e.true_val_.expr_)
        || boost::apply_visitor(*this, e.false_val_.expr_);
    }

    bool var_occurs_vis::operator()(const binary_op& e) const {
      return boost::apply_visitor(*this, e.left.expr_)
        || boost::apply_visitor(*this, e.right.expr_);
    }

    bool var_occurs_vis::operator()(const unary_op& e) const {
      return boost::apply_visitor(*this, e.subject.expr_);
    }

  }
}
#endif
