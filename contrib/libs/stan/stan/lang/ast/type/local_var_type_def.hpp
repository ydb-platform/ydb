#ifndef STAN_LANG_AST_LOCAL_VAR_TYPE_DEF_HPP
#define STAN_LANG_AST_LOCAL_VAR_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/get.hpp>

#include <ostream>
#include <string>
#include <vector>

namespace stan {
namespace lang {

local_var_type::local_var_type() : var_type_(ill_formed_type()) {}

local_var_type::local_var_type(const local_var_type& x)
    : var_type_(x.var_type_) {}

local_var_type::local_var_type(const local_t& x) : var_type_(x) {}

local_var_type::local_var_type(const ill_formed_type& x) : var_type_(x) {}

local_var_type::local_var_type(const int_type& x) : var_type_(x) {}

local_var_type::local_var_type(const double_type& x) : var_type_(x) {}

local_var_type::local_var_type(const vector_local_type& x) : var_type_(x) {}

local_var_type::local_var_type(const row_vector_local_type& x) : var_type_(x) {}

local_var_type::local_var_type(const matrix_local_type& x) : var_type_(x) {}

local_var_type::local_var_type(const local_array_type& x) : var_type_(x) {}

expression local_var_type::arg1() const {
  var_type_arg1_vis vis;
  return boost::apply_visitor(vis, var_type_);
}

expression local_var_type::arg2() const {
  var_type_arg2_vis vis;
  return boost::apply_visitor(vis, var_type_);
}

local_var_type local_var_type::array_contains() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_)) {
    local_array_type vt = boost::get<stan::lang::local_array_type>(var_type_);
    return vt.contains();
  }
  return ill_formed_type();
}

int local_var_type::array_dims() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_)) {
    local_array_type vt = boost::get<stan::lang::local_array_type>(var_type_);
    return vt.dims();
  }
  return 0;
}

local_var_type local_var_type::array_element_type() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_)) {
    local_array_type vt = boost::get<stan::lang::local_array_type>(var_type_);
    return vt.element_type();
  }
  return ill_formed_type();
}

expression local_var_type::array_len() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_)) {
    local_array_type vt = boost::get<stan::lang::local_array_type>(var_type_);
    return vt.array_len();
  }
  return expression(nil());
}

std::vector<expression> local_var_type::array_lens() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_)) {
    local_array_type vt = boost::get<stan::lang::local_array_type>(var_type_);
    return vt.array_lens();
  }
  return std::vector<expression>();
}

bare_expr_type local_var_type::bare_type() const {
  bare_type_vis vis;
  return boost::apply_visitor(vis, var_type_);
}

local_var_type local_var_type::innermost_type() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_)) {
    local_array_type vt = boost::get<stan::lang::local_array_type>(var_type_);
    return vt.contains();
  }
  return var_type_;
}


bool local_var_type::is_array_type() const {
  if (boost::get<stan::lang::local_array_type>(&var_type_))
    return true;
  return false;
}

std::string local_var_type::name() const {
  var_type_name_vis vis;
  return boost::apply_visitor(vis, var_type_);
}

int local_var_type::num_dims() const {
  return this->bare_type().num_dims();
}

std::ostream& operator<<(std::ostream& o, const local_var_type& var_type) {
  write_bare_expr_type(o, var_type.bare_type());
  return o;
}
}  // namespace lang
}  // namespace stan
#endif
