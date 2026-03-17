#ifndef STAN_LANG_AST_FUN_WRITE_EXPRESSION_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_WRITE_EXPRESSION_VIS_DEF_HPP

#include <stan/lang/ast.hpp>
#include <boost/lexical_cast.hpp>
#include <sstream>
#include <string>
#include <vector>

namespace stan {
namespace lang {
write_expression_vis::write_expression_vis() {}

std::string write_expression_vis::operator()(const nil& st) const {
  return "nil";
}

std::string write_expression_vis::operator()(const int_literal& e) const {
  return boost::lexical_cast<std::string>(e.val_);
}

std::string write_expression_vis::operator()(const double_literal& e) const {
  return e.string_;
}

std::string write_expression_vis::operator()(const array_expr& e) const {
  std::stringstream ss;
  ss << "{ ";
  for (size_t i = 0; i < e.args_.size(); ++i) {
    if (i > 0)
      ss << ", ";
    ss << e.args_[i].to_string();
  }
  ss << " }";
  return ss.str();
}

std::string write_expression_vis::operator()(const matrix_expr& e) const {
  std::stringstream ss;
  ss << "[ ";
  for (size_t i = 0; i < e.args_.size(); ++i) {
    if (i > 0)
      ss << ", ";
    ss << e.args_[i].to_string();
  }
  ss << " ]";
  return ss.str();
}

std::string write_expression_vis::operator()(const row_vector_expr& e) const {
  std::stringstream ss;
  ss << "[ ";
  for (size_t i = 0; i < e.args_.size(); ++i) {
    if (i > 0)
      ss << ", ";
    ss << e.args_[i].to_string();
  }
  ss << " ]";
  return ss.str();
}

std::string write_expression_vis::operator()(const variable& e) const {
  return e.name_;
}

std::string write_expression_vis::operator()(const fun& e) const {
  std::stringstream ss;
  if (e.original_name_.size() > 0)
    ss << e.original_name_;
  else
    ss << e.name_;
  ss << "(";
  for (size_t i = 0; i < e.args_.size(); ++i) {
    if (i > 0)
      ss << ", ";
    ss << e.args_[i].to_string();
  }
  ss << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(const integrate_1d& e) const {
  std::stringstream ss;
  ss << e.function_name_ << "(" << e.lb_.to_string() << ", "
     << e.ub_.to_string() << ", " << e.theta_.to_string() << ", "
     << e.x_r_.to_string() << ", " << e.x_i_.to_string() << ", "
     << e.rel_tol_.to_string() << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(const integrate_ode& e) const {
  std::stringstream ss;
  ss << e.integration_function_name_ << "(" << e.system_function_name_ << ", "
     << e.y0_.to_string() << ", " << e.t0_.to_string() << ", "
     << e.ts_.to_string() << ", " << e.x_.to_string() << ", "
     << e.x_int_.to_string() << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(
    const integrate_ode_control& e) const {
  std::stringstream ss;
  ss << e.integration_function_name_ << "(" << e.system_function_name_ << ", "
     << e.y0_.to_string() << ", " << e.t0_.to_string() << ", "
     << e.ts_.to_string() << ", " << e.x_.to_string() << ", "
     << e.x_int_.to_string() << ", " << e.rel_tol_.to_string() << ", "
     << e.abs_tol_.to_string() << ", " << e.max_num_steps_.to_string() << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(const algebra_solver& e) const {
  std::stringstream ss;
  ss << e.system_function_name_ << ", " << e.y_.to_string() << ", "
     << e.theta_.to_string() << ", " << e.x_r_.to_string() << ", "
     << e.x_i_.to_string() << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(
    const algebra_solver_control& e) const {
  std::stringstream ss;
  ss << e.system_function_name_ << ", " << e.y_.to_string() << ", "
     << e.theta_.to_string() << ", " << e.x_r_.to_string() << ", "
     << e.x_i_.to_string() << ", " << e.rel_tol_.to_string() << ", "
     << e.fun_tol_.to_string() << ", " << e.max_num_steps_.to_string() << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(const map_rect& e) const {
  std::stringstream ss;
  ss << e.call_id_ << ", " << e.fun_name_ << ", "
     << e.shared_params_.to_string() << ", " << e.job_params_.to_string()
     << ", " << e.job_data_r_.to_string() << ", " << e.job_data_i_.to_string()
     << ")";
  return ss.str();
}

std::string write_expression_vis::operator()(const index_op& e) const {
  std::stringstream ss;
  ss << e.expr_.to_string() << "[";
  for (size_t i = 0; i < e.dimss_.size(); ++i) {
    if (i > 0)
      ss << ", ";
    if (e.dimss_[i].size() == 1) {
      ss << e.dimss_[i][0].to_string();
    } else {
      ss << "[";
      for (size_t j = 0; j < e.dimss_[i].size(); ++j) {
        if (j > 0)
          ss << ", ";
        ss << e.dimss_[i][j].to_string();
      }
      ss << "]";
    }
  }
  ss << "]";
  return ss.str();
}

std::string write_expression_vis::operator()(const index_op_sliced& e) const {
  std::stringstream ss;
  ss << e.expr_.to_string() << "[";
  for (size_t i = 0; i < e.idxs_.size(); ++i) {
    if (i > 0)
      ss << ", ";
    ss << e.idxs_[i].to_string();
  }
  ss << "]";
  return ss.str();
}

std::string write_expression_vis::operator()(const conditional_op& e) const {
  std::stringstream ss;
  ss << e.cond_.to_string() << " ? " << e.true_val_.to_string() << " : "
     << e.false_val_.to_string();
  return ss.str();
}

std::string write_expression_vis::operator()(const binary_op& e) const {
  std::stringstream ss;
  ss << e.left.to_string() << " " << e.op << " " << e.right.to_string();
  return ss.str();
}

std::string write_expression_vis::operator()(const unary_op& e) const {
  std::stringstream ss;
  ss << e.op << e.subject.to_string();
  return ss.str();
}

}  // namespace lang
}  // namespace stan
#endif
