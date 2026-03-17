#ifndef STAN_LANG_GRAMMARS_SEMANTIC_ACTIONS_DEF_CPP
#define STAN_LANG_GRAMMARS_SEMANTIC_ACTIONS_DEF_CPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/grammars/iterator_typedefs.hpp>
#include <stan/lang/grammars/semantic_actions.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/spirit/include/qi.hpp>
#include <boost/variant/apply_visitor.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <climits>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <typeinfo>

namespace stan {

namespace lang {

/**
 * Add namespace qualifier `stan::math::` or `std::` to function names
 * in order to avoid ambiguities for functions in the Stan language which
 * are also defined in c and/or other libraries that some compilers (gcc)
 * bring into the top-level namespace.
 *
 * @param[in, out] f Function to qualify.
 */
void qualify_builtins(fun &f) {
  if ((f.name_ == "max" || f.name_ == "min") && f.args_.size() == 2 &&
      f.args_[0].bare_type().is_int_type() &&
      f.args_[1].bare_type().is_int_type()) {
    f.name_ = "std::" + f.name_;
    return;
  }

  if (f.name_ == "ceil" && f.args_[0].bare_type().is_int_type()) {
    f.name_ = "std::" + f.name_;
    return;
  }

  if ((f.args_.size() == 0 &&
       (f.name_ == "e" || f.name_ == "pi" || f.name_ == "log2" ||
        f.name_ == "log10" || f.name_ == "sqrt2" || f.name_ == "not_a_number" ||
        f.name_ == "positive_infinity" || f.name_ == "negative_infinity" ||
        f.name_ == "machine_precision")) ||
      (f.args_.size() == 1 &&
       (f.name_ == "abs" || f.name_ == "acos" || f.name_ == "acosh" ||
        f.name_ == "asin" || f.name_ == "asinh" || f.name_ == "atan" ||
        f.name_ == "atan2" || f.name_ == "atanh" || f.name_ == "cbrt" ||
        f.name_ == "ceil" || f.name_ == "cos" || f.name_ == "cosh" ||
        f.name_ == "erf" || f.name_ == "erfc" || f.name_ == "exp" ||
        f.name_ == "exp2" || f.name_ == "expm1" || f.name_ == "fabs" ||
        f.name_ == "floor" || f.name_ == "lgamma" || f.name_ == "log" ||
        f.name_ == "log1p" || f.name_ == "log2" || f.name_ == "log10" ||
        f.name_ == "round" || f.name_ == "sin" || f.name_ == "sinh" ||
        f.name_ == "sqrt" || f.name_ == "tan" || f.name_ == "tanh" ||
        f.name_ == "tgamma" || f.name_ == "trunc")) ||
      (f.args_.size() == 2 && (f.name_ == "fdim" || f.name_ == "fmax" ||
                               f.name_ == "fmin" || f.name_ == "hypot")) ||
      (f.args_.size() == 3 && f.name_ == "fma"))
    f.name_ = "stan::math::" + f.name_;
}

bool has_prob_suffix(const std::string &s) {
  return ends_with("_lpdf", s) || ends_with("_lpmf", s) ||
         ends_with("_lcdf", s) || ends_with("_lccdf", s);
}

bool has_rng_lp_suffix(const std::string &s) {
  return ends_with("_lp", s) || ends_with("_rng", s);
}

void replace_suffix(const std::string &old_suffix,
                    const std::string &new_suffix, fun &f) {
  if (!ends_with(old_suffix, f.name_))
    return;
  f.name_ = f.name_.substr(0, f.name_.size() - old_suffix.size()) + new_suffix;
}

bool deprecate_fun(const std::string &old_name, const std::string &new_name,
                   fun &f, std::ostream &msgs) {
  if (f.name_ != old_name)
    return false;
  f.original_name_ = f.name_;
  f.name_ = new_name;
  msgs << "Info: Function name '" << old_name << "' is deprecated"
       << " and will be removed in a later release; please replace"
       << " with '" << new_name << "'" << std::endl;
  return true;
}

bool deprecate_suffix(const std::string &deprecated_suffix,
                      const std::string &replacement, fun &f,
                      std::ostream &msgs) {
  if (!ends_with(deprecated_suffix, f.name_))
    return false;
  msgs << "Info: Deprecated function '" << f.name_ << "';"
       << " please replace suffix '" << deprecated_suffix << "' with "
       << replacement << std::endl;
  return true;
}

void set_fun_type(fun &fun, std::ostream &error_msgs) {
  std::vector<bare_expr_type> arg_types;
  for (size_t i = 0; i < fun.args_.size(); ++i)
    arg_types.push_back(fun.args_[i].bare_type());
  fun.type_ = function_signatures::instance().get_result_type(
      fun.name_, arg_types, error_msgs);
}

int num_dimss(std::vector<std::vector<stan::lang::expression>> &dimss) {
  int sum = 0;
  for (size_t i = 0; i < dimss.size(); ++i)
    sum += dimss[i].size();
  return sum;
}

bool is_double_return(const std::string &function_name,
                      const std::vector<bare_expr_type> &arg_types,
                      std::ostream &error_msgs) {
  return function_signatures::instance()
      .get_result_type(function_name, arg_types, error_msgs, true)
      .is_double_type();
}

bool is_univariate(const bare_expr_type &et) {
  return et.num_dims() == 0 && (et.is_int_type() || et.is_double_type());
}

bool can_assign_to_lhs_var(const std::string &lhs_var_name,
                           const scope &var_scope, const variable_map &vm,
                           std::ostream &error_msgs) {
  if (lhs_var_name == std::string("lp__")) {
    error_msgs << std::endl
               << "Error (fatal):  Use of lp__ is no longer supported."
               << std::endl
               << "  Use target += ... statement to increment log density."
               << std::endl;
    return false;
  }
  if (!vm.exists(lhs_var_name)) {
    error_msgs << "Unknown variable in assignment"
               << "; lhs variable=" << lhs_var_name << std::endl;
    return false;
  }
  scope lhs_origin = vm.get_scope(lhs_var_name);
  // enforce constancy of loop variables
  if (lhs_origin.program_block() == loop_identifier_origin) {
    error_msgs << "Loop variable " << lhs_var_name
               << " cannot be used on left side of assignment statement."
               << std::endl;
    return false;
  }
  // enforce constancy of function args
  if (!lhs_origin.is_local() && lhs_origin.fun()) {
    error_msgs << "Cannot assign to function argument variables." << std::endl
               << "Use local variables instead." << std::endl;
    return false;
  }
  if (lhs_origin.program_block() != var_scope.program_block()) {
    error_msgs << "Cannot assign to variable outside of declaration block"
               << "; left-hand-side variable origin=";
    print_scope(error_msgs, lhs_origin);
    error_msgs << std::endl;
    return false;
  }
  return true;
}

bare_expr_type infer_var_dims_type(const bare_expr_type &var_type,
                                   const variable_dims &var_dims) {
  size_t num_index_dims = var_dims.dims_.size();
  return infer_type_indexing(var_type, num_index_dims);
}

bool has_same_shape(const bare_expr_type &lhs_type, const expression &rhs_expr,
                    const std::string &name, const std::string &stmt_type,
                    std::ostream &error_msgs) {
  if (lhs_type.num_dims() != rhs_expr.bare_type().num_dims() ||
      lhs_type.array_dims() != rhs_expr.bare_type().array_dims()) {
    error_msgs << "Dimension mismatch in " << stmt_type
               << "; variable name = " << name << ", type = " << lhs_type
               << "; right-hand side type = " << rhs_expr.bare_type() << "."
               << std::endl;
    return false;
  }

  // allow int -> double promotion, even in arrays
  bool types_compatible =
      (lhs_type.innermost_type() == rhs_expr.bare_type().innermost_type() ||
       (lhs_type.innermost_type().is_double_type() &&
        rhs_expr.bare_type().innermost_type().is_int_type()));
  if (!types_compatible) {
    error_msgs << "Base type mismatch in " << stmt_type
               << "; variable name = " << name
               << ", base type = " << lhs_type.innermost_type()
               << "; right-hand side base type = "
               << rhs_expr.bare_type().innermost_type() << "." << std::endl;
    return false;
  }
  return true;
}

//  //////////////////////////////////
// *** functors for grammar rules ***
//  //////////////////////////////////

void validate_double_expr::operator()(const expression &expr, bool &pass,
                                      std::stringstream &error_msgs) const {
  if (!expr.bare_type().is_double_type() && !expr.bare_type().is_int_type()) {
    error_msgs << "Expression denoting real required; found type="
               << expr.bare_type() << "." << std::endl;
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<validate_double_expr> validate_double_expr_f;

template <typename L, typename R>
void assign_lhs::operator()(L &lhs, const R &rhs) const {
  lhs = rhs;
}
boost::phoenix::function<assign_lhs> assign_lhs_f;

template void assign_lhs::operator()(expression &, const expression &) const;
template void assign_lhs::operator()(expression &,
                                     const double_literal &) const;
template void assign_lhs::operator()(expression &, const int_literal &) const;
template void assign_lhs::operator()(expression &, const integrate_1d &) const;
template void assign_lhs::operator()(expression &, const integrate_ode &) const;
template void assign_lhs::operator()(expression &,
                                     const integrate_ode_control &) const;
template void assign_lhs::operator()(expression &,
                                     const algebra_solver &) const;
template void assign_lhs::operator()(expression &,
                                     const algebra_solver_control &) const;
template void assign_lhs::operator()(expression &, const map_rect &) const;
template void assign_lhs::operator()(array_expr &, const array_expr &) const;
template void assign_lhs::operator()(matrix_expr &, const matrix_expr &) const;
template void assign_lhs::operator()(row_vector_expr &,
                                     const row_vector_expr &) const;
template void assign_lhs::operator()(int &, const int &) const;
template void assign_lhs::operator()(size_t &, const size_t &) const;
template void assign_lhs::operator()(statement &, const statement &) const;
template void assign_lhs::operator()(std::vector<var_decl> &,
                                     const std::vector<var_decl> &) const;
template void assign_lhs::operator()(std::vector<idx> &,
                                     const std::vector<idx> &) const;
template void assign_lhs::
operator()(std::vector<std::vector<expression>> &,
           const std::vector<std::vector<expression>> &) const;
template void assign_lhs::operator()(fun &, const fun &) const;
template void assign_lhs::operator()(variable &, const variable &) const;
template void assign_lhs::operator()(std::vector<block_var_decl> &,
                                     const std::vector<block_var_decl> &) const;
template void assign_lhs::operator()(std::vector<local_var_decl> &,
                                     const std::vector<local_var_decl> &) const;
template void assign_lhs::operator()(bare_expr_type &,
                                     const bare_expr_type &) const;
template void assign_lhs::operator()(block_var_decl &,
                                     const block_var_decl &) const;
template void assign_lhs::operator()(local_var_decl &,
                                     const local_var_decl &) const;
template void assign_lhs::operator()(var_decl &, const var_decl &) const;

void validate_expr_type3::operator()(const expression &expr, bool &pass,
                                     std::ostream &error_msgs) const {
  pass = !expr.bare_type().is_ill_formed_type();
  if (!pass)
    error_msgs << "Expression is ill formed." << std::endl;
}
boost::phoenix::function<validate_expr_type3> validate_expr_type3_f;

void is_prob_fun::operator()(const std::string &s, bool &pass) const {
  pass = has_prob_suffix(s);
}
boost::phoenix::function<is_prob_fun> is_prob_fun_f;

void addition_expr3::operator()(expression &expr1, const expression &expr2,
                                std::ostream &error_msgs) const {
  if (expr1.bare_type().is_primitive() && expr2.bare_type().is_primitive()) {
    expr1 += expr2;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("add", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<addition_expr3> addition3_f;

void subtraction_expr3::operator()(expression &expr1, const expression &expr2,
                                   std::ostream &error_msgs) const {
  if (expr1.bare_type().is_primitive() && expr2.bare_type().is_primitive()) {
    expr1 -= expr2;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("subtract", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<subtraction_expr3> subtraction3_f;

void increment_size_t::operator()(size_t &lhs) const { ++lhs; }
boost::phoenix::function<increment_size_t> increment_size_t_f;

void validate_conditional_op::operator()(conditional_op &conditional_op,
                                         const scope &var_scope, bool &pass,
                                         const variable_map &var_map,
                                         std::ostream &error_msgs) const {
  bare_expr_type cond_type = conditional_op.cond_.bare_type();
  if (!cond_type.is_int_type()) {
    error_msgs << "Condition in ternary expression must be"
               << " primitive int;"
               << " found type=" << cond_type << "." << std::endl;
    pass = false;
    return;
  }

  bare_expr_type true_val_type = conditional_op.true_val_.bare_type();
  bare_expr_type false_val_type = conditional_op.false_val_.bare_type();

  bool types_compatible =
      (true_val_type == false_val_type) ||
      (true_val_type.is_double_type() && false_val_type.is_int_type()) ||
      (true_val_type.is_int_type() && false_val_type.is_double_type());

  if (!types_compatible) {
    error_msgs << "Type mismatch in ternary expression,"
               << " expression when true is: ";
    write_bare_expr_type(error_msgs, true_val_type);
    error_msgs << "; expression when false is: ";
    write_bare_expr_type(error_msgs, false_val_type);
    error_msgs << "." << std::endl;
    pass = false;
    return;
  }

  if (true_val_type.is_primitive())
    conditional_op.type_ =
        (true_val_type == false_val_type) ? true_val_type : double_type();
  else
    conditional_op.type_ = true_val_type;

  conditional_op.has_var_ = has_var(conditional_op, var_map);
  conditional_op.scope_ = var_scope;
  pass = true;
}
boost::phoenix::function<validate_conditional_op> validate_conditional_op_f;

void binary_op_expr::operator()(expression &expr1, const expression &expr2,
                                const std::string &op,
                                const std::string &fun_name,
                                std::ostream &error_msgs) const {
  if (!expr1.bare_type().is_primitive() || !expr2.bare_type().is_primitive()) {
    error_msgs << "Binary infix operator " << op
               << " with functional interpretation " << fun_name
               << " requires arguments of primitive type (int or real)"
               << ", found left type=" << expr1.bare_type()
               << ", right arg type=" << expr2.bare_type() << "." << std::endl;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f(fun_name, args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<binary_op_expr> binary_op_f;

void validate_non_void_arg_function::
operator()(bare_expr_type &arg_type, const scope &var_scope, bool &pass,
           std::ostream &error_msgs) const {
  if (var_scope.program_block() == data_origin)
    arg_type.set_is_data();
  pass = !arg_type.is_void_type();
  if (!pass)
    error_msgs << "Functions cannot contain void argument types; "
               << "found void argument." << std::endl;
}
boost::phoenix::function<validate_non_void_arg_function>
    validate_non_void_arg_f;

void set_void_function::operator()(const bare_expr_type &return_type,
                                   scope &var_scope, bool &pass,
                                   std::ostream &error_msgs) const {
  if (return_type.is_void_type() && return_type.num_dims() > 0) {
    error_msgs << "Void return type may not have dimensions declared."
               << std::endl;
    pass = false;
    return;
  }
  var_scope = return_type.is_void_type() ? scope(void_function_argument_origin)
                                         : scope(function_argument_origin);
  pass = true;
}
boost::phoenix::function<set_void_function> set_void_function_f;

void set_allows_sampling_origin::operator()(const std::string &identifier,
                                            scope &var_scope) const {
  if (ends_with("_lp", identifier)) {
    var_scope = var_scope.void_fun() ? scope(void_function_argument_origin_lp)
                                     : scope(function_argument_origin_lp);
  } else if (ends_with("_rng", identifier)) {
    var_scope = var_scope.void_fun() ? scope(void_function_argument_origin_rng)
                                     : scope(function_argument_origin_rng);
  } else {
    var_scope = var_scope.void_fun() ? scope(void_function_argument_origin)
                                     : scope(function_argument_origin);
  }
}
boost::phoenix::function<set_allows_sampling_origin>
    set_allows_sampling_origin_f;

void validate_declarations::
operator()(bool &pass,
           std::set<std::pair<std::string, function_signature_t>> &declared,
           std::set<std::pair<std::string, function_signature_t>> &defined,
           std::ostream &error_msgs, bool allow_undefined) const {
  using std::pair;
  using std::set;
  using std::string;
  typedef set<pair<string, function_signature_t>>::iterator iterator_t;
  if (!allow_undefined) {
    for (iterator_t it = declared.begin(); it != declared.end(); ++it) {
      if (defined.find(*it) == defined.end()) {
        error_msgs << "Function declared, but not defined."
                   << " Function name=" << (*it).first << std::endl;
        pass = false;
        return;
      }
    }
  }
  pass = true;
}
boost::phoenix::function<validate_declarations> validate_declarations_f;

bool fun_exists(
    const std::set<std::pair<std::string, function_signature_t>> &existing,
    const std::pair<std::string, function_signature_t> &name_sig,
    bool name_only = true) {
  for (std::set<std::pair<std::string, function_signature_t>>::const_iterator
           it = existing.begin();
       it != existing.end(); ++it) {
    if (name_sig.first == (*it).first &&
        (name_only || name_sig.second.second == (*it).second.second))
      return true;  // name and arg sequences match
  }
  return false;
}

void validate_prob_fun::operator()(std::string &fname, bool &pass,
                                   std::ostream &error_msgs) const {
  if (has_prob_fun_suffix(fname)) {
    std::string dist_name = strip_prob_fun_suffix(fname);
    if (!fun_name_exists(fname)  // catch redefines later avoid fwd
        && (fun_name_exists(dist_name + "_lpdf") ||
            fun_name_exists(dist_name + "_lpmf") ||
            fun_name_exists(dist_name + "_log"))) {
      error_msgs << "Parse Error.  Probability function already defined"
                 << " for " << dist_name << std::endl;
      pass = false;
      return;
    }
  }
  if (has_cdf_suffix(fname)) {
    std::string dist_name = strip_cdf_suffix(fname);
    if (fun_name_exists(dist_name + "_cdf_log") ||
        fun_name_exists(dist_name + "_lcdf")) {
      error_msgs << " Parse Error.  CDF already defined for " << dist_name
                 << std::endl;
      pass = false;
      return;
    }
  }
  if (has_ccdf_suffix(fname)) {
    std::string dist_name = strip_ccdf_suffix(fname);
    if (fun_name_exists(dist_name + "_ccdf_log") ||
        fun_name_exists(dist_name + "_lccdf")) {
      error_msgs << " Parse Error.  CCDF already defined for " << dist_name
                 << std::endl;
      pass = false;
      return;
    }
  }
}
boost::phoenix::function<validate_prob_fun> validate_prob_fun_f;

void add_function_signature::operator()(
    const function_decl_def &decl, bool &pass,
    std::set<std::pair<std::string, function_signature_t>> &functions_declared,
    std::set<std::pair<std::string, function_signature_t>> &functions_defined,
    std::ostream &error_msgs) const {
  std::vector<bare_expr_type> arg_types;
  for (size_t i = 0; i < decl.arg_decls_.size(); ++i) {
    arg_types.push_back(decl.arg_decls_[i].bare_type());
  }
  function_signature_t sig(decl.return_type_, arg_types);
  std::pair<std::string, function_signature_t> name_sig(decl.name_, sig);

  // check that not already declared if just declaration
  if (decl.body_.is_no_op_statement() &&
      fun_exists(functions_declared, name_sig)) {
    error_msgs << "Parse Error.  Function already declared, name="
               << decl.name_;
    pass = false;
    return;
  }

  // check not already user defined
  if (fun_exists(functions_defined, name_sig)) {
    error_msgs << "Parse Error.  Function already defined, name=" << decl.name_;
    pass = false;
    return;
  }

  // check not already system defined
  if (!fun_exists(functions_declared, name_sig) &&
      function_signatures::instance().is_defined(decl.name_, sig)) {
    error_msgs << "Parse Error.  Function system defined, name=" << decl.name_;
    pass = false;
    return;
  }

  // check argument type and qualifiers
  if (!decl.body_.is_no_op_statement()) {
    function_signature_t decl_sig =
        function_signatures::instance().get_definition(decl.name_, sig);
    if (!decl_sig.first.is_ill_formed_type()) {
      for (size_t i = 0; i < decl.arg_decls_.size(); ++i) {
        if (decl_sig.second[i] != arg_types[i] ||
            decl_sig.second[i].is_data() != arg_types[i].is_data()) {
          error_msgs << "Declaration doesn't match definition "
                     << "for function: " << decl.name_ << " argument "
                     << (i + 1) << ": argument declared as " << arg_types[i]
                     << ", defined as " << decl_sig.second[i] << "."
                     << std::endl;
          pass = false;
          return;
        }
      }
    }
  }

  if (ends_with("_lpdf", decl.name_) &&
      arg_types[0].innermost_type().is_int_type()) {
    error_msgs << "Parse Error.  Probability density functions require"
               << " real variates (first argument)."
               << " Found type = " << arg_types[0] << std::endl;
    pass = false;
    return;
  }
  if (ends_with("_lpmf", decl.name_) &&
      !arg_types[0].innermost_type().is_int_type()) {
    error_msgs << "Parse Error.  Probability mass functions require"
               << " integer variates (first argument)."
               << " Found type = " << arg_types[0] << std::endl;
    pass = false;
    return;
  }

  // add declaration in local sets and in parser function sigs
  if (functions_declared.find(name_sig) == functions_declared.end()) {
    functions_declared.insert(name_sig);
    function_signatures::instance().add(decl.name_, decl.return_type_,
                                        arg_types);
    function_signatures::instance().set_user_defined(name_sig);
  }

  // add as definition if there's a body
  if (!decl.body_.is_no_op_statement())
    functions_defined.insert(name_sig);
  pass = true;
}
boost::phoenix::function<add_function_signature> add_function_signature_f;

void validate_pmf_pdf_variate::operator()(function_decl_def &decl, bool &pass,
                                          std::ostream &error_msgs) const {
  if (!has_prob_fun_suffix(decl.name_))
    return;
  if (decl.arg_decls_.size() == 0) {
    error_msgs << "Parse Error.  Probability functions require"
               << " at least one argument." << std::endl;
    pass = false;
    return;
  }
  bare_expr_type variate_type = decl.arg_decls_[0].bare_type().innermost_type();
  if (ends_with("_lpdf", decl.name_) && variate_type.is_int_type()) {
    error_msgs << "Parse Error.  Probability density functions require"
               << " real variates (first argument)."
               << " Found type = " << variate_type << std::endl;
    pass = false;
    return;
  }
  if (ends_with("_lpmf", decl.name_) && !variate_type.is_int_type()) {
    error_msgs << "Parse Error.  Probability mass functions require"
               << " integer variates (first argument)."
               << " Found type = " << variate_type << std::endl;
    pass = false;
    return;
  }
}
boost::phoenix::function<validate_pmf_pdf_variate> validate_pmf_pdf_variate_f;

void validate_return_type::operator()(function_decl_def &decl, bool &pass,
                                      std::ostream &error_msgs) const {
  pass = decl.body_.is_no_op_statement() ||
         stan::lang::returns_type(decl.return_type_, decl.body_, error_msgs);
  if (!pass) {
    error_msgs << "Improper return in body of function." << std::endl;
    return;
  }

  if ((ends_with("_log", decl.name_) || ends_with("_lpdf", decl.name_) ||
       ends_with("_lpmf", decl.name_) || ends_with("_lcdf", decl.name_) ||
       ends_with("_lccdf", decl.name_)) &&
      !decl.return_type_.is_double_type()) {
    pass = false;
    error_msgs << "Real return type required for probability functions"
               << " ending in _log, _lpdf, _lpmf, _lcdf, or _lccdf."
               << std::endl;
  }
}
boost::phoenix::function<validate_return_type> validate_return_type_f;

void set_fun_params_scope::operator()(scope &var_scope,
                                      variable_map &vm) const {
  var_scope = scope(var_scope.program_block(), true);
  // generated log_prob code has vector called "params_r__"
  // hidden way to get unconstrained params from model
  vm.add("params_r__", var_decl("params_r__", vector_type()), parameter_origin);
}
boost::phoenix::function<set_fun_params_scope> set_fun_params_scope_f;

void unscope_variables::operator()(function_decl_def &decl,
                                   variable_map &vm) const {
  vm.remove("params_r__");
  for (size_t i = 0; i < decl.arg_decls_.size(); ++i)
    vm.remove(decl.arg_decls_[i].name());
}
boost::phoenix::function<unscope_variables> unscope_variables_f;

void add_fun_arg_var::operator()(const var_decl &decl, const scope &scope,
                                 bool &pass, variable_map &vm,
                                 std::ostream &error_msgs) const {
  if (vm.exists(decl.name())) {
    pass = false;
    error_msgs << "Duplicate declaration of variable, name=" << decl.name()
               << "; attempt to redeclare as function argument"
               << "; original declaration as ";
    print_scope(error_msgs, vm.get_scope(decl.name()));
    error_msgs << " variable." << std::endl;
    return;
  }
  pass = true;
  stan::lang::scope arg_scope(scope.program_block() == data_origin
                                  ? data_origin
                                  : function_argument_origin);
  vm.add(decl.name(), decl, arg_scope);
}
boost::phoenix::function<add_fun_arg_var> add_fun_arg_var_f;

// TODO(carpenter): seems redundant; see if it can be removed
void set_omni_idx::operator()(omni_idx &val) const { val = omni_idx(); }
boost::phoenix::function<set_omni_idx> set_omni_idx_f;

void validate_int_expr_silent::operator()(const expression &e,
                                          bool &pass) const {
  pass = e.bare_type().is_int_type();
}
boost::phoenix::function<validate_int_expr_silent> validate_int_expr_silent_f;

void validate_ints_expression::operator()(const expression &e, bool &pass,
                                          std::ostream &error_msgs) const {
  if (!e.bare_type().innermost_type().is_int_type()) {
    error_msgs << "Container index must be integer; found type="
               << e.bare_type() << std::endl;
    pass = false;
    return;
  }
  if (e.bare_type().num_dims() > 1) {
    // tests > 1 so that message is coherent because the single
    // integer array tests don't print
    error_msgs << "Index must be integer or 1D integer array;"
               << " found number of dimensions=" << e.bare_type().num_dims()
               << std::endl;
    pass = false;
    return;
  }
  if (e.bare_type().num_dims() == 0) {
    // not an array expression, fail and backtrack
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<validate_ints_expression> validate_ints_expression_f;

void add_params_var::operator()(variable_map &vm) const {
  vm.add("params_r__", var_decl("params_r__", vector_type()),
         parameter_origin);  // acts like a parameter
}
boost::phoenix::function<add_params_var> add_params_var_f;

void remove_params_var::operator()(variable_map &vm) const {
  vm.remove("params_r__");
}
boost::phoenix::function<remove_params_var> remove_params_var_f;

void dump_program_line(size_t idx_errline, int offset,
                       const std::string &origin_file, size_t origin_line,
                       const io::program_reader &reader,
                       const std::vector<std::string> &program_lines,
                       std::stringstream &error_msgs) {
  boost::format fmt_lineno("%6d: ");
  if (idx_errline + offset > 0 && idx_errline + offset < program_lines.size()) {
    io::program_reader::trace_t trace = reader.trace(idx_errline + offset);
    if (trace[trace.size() - 1].first == origin_file) {
      std::string lineno = str(fmt_lineno % (origin_line + offset));
      error_msgs << lineno << program_lines[idx_errline + offset - 1]
                 << std::endl;
    }
  }
}

void program_error::operator()(pos_iterator_t begin, pos_iterator_t end,
                               pos_iterator_t where, variable_map &vm,
                               std::stringstream &error_msgs,
                               const io::program_reader &reader) const {
  // extract line and column of error
  size_t idx_errline = boost::spirit::get_line(where);
  if (idx_errline == 0) {
    error_msgs << "Error before start of program." << std::endl;
    return;
  }
  size_t idx_errcol = 0;
  idx_errcol = get_column(begin, where) - 1;

  // extract lines of included program
  std::basic_stringstream<char> program_ss;
  program_ss << boost::make_iterator_range(begin, end);
  std::vector<std::string> program_lines;
  while (!program_ss.eof()) {
    std::string line;
    std::getline(program_ss, line);
    program_lines.push_back(line);
  }

  // dump include trace for error line
  io::program_reader::trace_t trace = reader.trace(idx_errline);
  std::string origin_file = trace[trace.size() - 1].first;
  size_t origin_line = trace[trace.size() - 1].second;
  error_msgs << " error in '" << trace[trace.size() - 1].first << "' at line "
             << trace[trace.size() - 1].second << ", column " << idx_errcol
             << std::endl;
  for (int i = trace.size() - 1; i-- > 0;)
    error_msgs << " included from '" << trace[i].first << "' at line "
               << trace[i].second << std::endl;

  // dump context of error
  error_msgs << "  -------------------------------------------------"
             << std::endl;

  dump_program_line(idx_errline, -2, origin_file, origin_line, reader,
                    program_lines, error_msgs);
  dump_program_line(idx_errline, -1, origin_file, origin_line, reader,
                    program_lines, error_msgs);
  dump_program_line(idx_errline, 0, origin_file, origin_line, reader,
                    program_lines, error_msgs);
  error_msgs << std::setw(idx_errcol + 8) << "^" << std::endl;
  dump_program_line(idx_errline, +1, origin_file, origin_line, reader,
                    program_lines, error_msgs);

  error_msgs << "  -------------------------------------------------"
             << std::endl
             << std::endl;
}
boost::phoenix::function<program_error> program_error_f;

void add_conditional_condition::
operator()(conditional_statement &cs, const expression &e, bool &pass,
           std::stringstream &error_msgs) const {
  if (!e.bare_type().is_primitive()) {
    error_msgs << "Conditions in if-else statement must be"
               << " primitive int or real;"
               << " found type=" << e.bare_type() << std::endl;
    pass = false;
    return;
  }
  cs.conditions_.push_back(e);
  pass = true;
  return;
}
boost::phoenix::function<add_conditional_condition> add_conditional_condition_f;

void add_conditional_body::operator()(conditional_statement &cs,
                                      const statement &s) const {
  cs.bodies_.push_back(s);
}
boost::phoenix::function<add_conditional_body> add_conditional_body_f;

void deprecate_old_assignment_op::operator()(std::string &op,
                                             std::ostream &error_msgs) const {
  error_msgs << "Info: assignment operator <- deprecated"
             << " in the Stan language;"
             << " use = instead." << std::endl;
  std::string eq("=");
  op = eq;
}
boost::phoenix::function<deprecate_old_assignment_op>
    deprecate_old_assignment_op_f;

void non_void_return_msg::operator()(scope var_scope, bool &pass,
                                     std::ostream &error_msgs) const {
  pass = false;
  if (var_scope.non_void_fun()) {
    error_msgs << "Non-void function must return expression"
               << " of specified return type." << std::endl;
    return;
  }
  error_msgs << "Return statement only allowed from function bodies."
             << std::endl;
}
boost::phoenix::function<non_void_return_msg> non_void_return_msg_f;

void validate_return_allowed::operator()(scope var_scope, bool &pass,
                                         std::ostream &error_msgs) const {
  if (var_scope.void_fun()) {
    error_msgs << "Void function cannot return a value." << std::endl;
    pass = false;
    return;
  }
  if (!var_scope.non_void_fun()) {
    error_msgs << "Returns only allowed from function bodies." << std::endl;
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<validate_return_allowed> validate_return_allowed_f;

void validate_void_return_allowed::operator()(scope var_scope, bool &pass,
                                              std::ostream &error_msgs) const {
  if (!var_scope.void_fun()) {
    error_msgs << "Void returns only allowed from function"
               << " bodies of void return type." << std::endl;
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<validate_void_return_allowed>
    validate_void_return_allowed_f;

void validate_lhs_var_assgn::operator()(assgn &a, const scope &var_scope,
                                        bool &pass, const variable_map &vm,
                                        std::ostream &error_msgs) const {
  std::string name(a.lhs_var_.name_);
  if (!can_assign_to_lhs_var(name, var_scope, vm, error_msgs)) {
    pass = false;
    return;
  }
  a.lhs_var_.set_type(vm.get_bare_type(name));
}
boost::phoenix::function<validate_lhs_var_assgn> validate_lhs_var_assgn_f;

void set_lhs_var_assgn::operator()(assgn &a, const std::string &name,
                                   bool &pass, const variable_map &vm) const {
  if (!vm.exists(name)) {
    pass = false;
    return;
  }
  a.lhs_var_ = variable(name);
  a.lhs_var_.set_type(vm.get_bare_type(name));
  pass = true;
}
boost::phoenix::function<set_lhs_var_assgn> set_lhs_var_assgn_f;

void validate_assgn::operator()(assgn &a, bool &pass, const variable_map &vm,
                                std::ostream &error_msgs) const {
  // validate lhs name + idxs
  std::string name = a.lhs_var_.name_;
  expression lhs_expr = expression(a.lhs_var_);
  bare_expr_type lhs_type = indexed_type(lhs_expr, a.idxs_);

  if (lhs_type.is_ill_formed_type()) {
    error_msgs << "Left-hand side indexing incompatible with variable."
               << std::endl;
    pass = false;
    return;
  }
  if (a.is_simple_assignment()) {
    if (!has_same_shape(lhs_type, a.rhs_, name, "assignment", error_msgs)) {
      pass = false;
      return;
    }
    pass = true;
    return;
  } else {
    // compound operator-assignment
    std::string op_equals = a.op_;
    a.op_ = op_equals.substr(0, op_equals.size() - 1);

    if (lhs_type.array_dims() > 0) {
      error_msgs << "Cannot apply operator '" << op_equals
                 << "' to array variable; variable name = " << name << ".";
      error_msgs << std::endl;
      pass = false;
      return;
    }

    bare_expr_type rhs_type = a.rhs_.bare_type();
    if (lhs_type.is_primitive() && boost::algorithm::starts_with(a.op_, ".")) {
      error_msgs << "Cannot apply element-wise operation to scalar"
                 << "; compound operator is: " << op_equals << std::endl;
      pass = false;
      return;
    }
    if (lhs_type.is_primitive() && rhs_type.is_primitive() &&
        (lhs_type.innermost_type().is_double_type() || lhs_type == rhs_type)) {
      pass = true;
      return;
    }

    std::string op_name;
    if (a.op_ == "+") {
      op_name = "add";
    } else if (a.op_ == "-") {
      op_name = "subtract";
    } else if (a.op_ == "*") {
      op_name = "multiply";
    } else if (a.op_ == "/") {
      op_name = "divide";
    } else if (a.op_ == "./") {
      op_name = "elt_divide";
    } else if (a.op_ == ".*") {
      op_name = "elt_multiply";
    }
    // check that "lhs <op> rhs" is valid stan::math function sig
    std::vector<bare_expr_type> arg_types;
    arg_types.push_back(bare_expr_type(lhs_type));
    arg_types.push_back(bare_expr_type(rhs_type));
    function_signature_t op_equals_sig(lhs_type, arg_types);
    if (!function_signatures::instance().is_defined(op_name, op_equals_sig)) {
      error_msgs << "Cannot apply operator '" << op_equals << "' to operands;"
                 << " left-hand side type = " << lhs_type
                 << "; right-hand side type=" << rhs_type << std::endl;
      pass = false;
      return;
    }
    a.op_name_ = op_name;
    pass = true;
  }
}
boost::phoenix::function<validate_assgn> validate_assgn_f;

void validate_sample::operator()(sample &s, const variable_map &var_map,
                                 bool &pass, std::ostream &error_msgs) const {
  std::vector<bare_expr_type> arg_types;
  arg_types.push_back(s.expr_.bare_type());
  for (size_t i = 0; i < s.dist_.args_.size(); ++i) {
    arg_types.push_back(s.dist_.args_[i].bare_type());
  }

  std::string function_name(s.dist_.family_);
  std::string internal_function_name = get_prob_fun(function_name);
  s.is_discrete_ = function_signatures::instance().discrete_first_arg(
      internal_function_name);

  if (internal_function_name.size() == 0) {
    pass = false;
    error_msgs << "Unknown distribution name: " << function_name << std::endl;
    return;
  }

  if (!(ends_with("_lpdf", internal_function_name) ||
        ends_with("_lpmf", internal_function_name) ||
        ends_with("_log", internal_function_name))) {
    pass = false;
    error_msgs << "Probability function must end in _lpdf or _lpmf."
               << " Found distribution family = " << function_name
               << " with no corresponding probability function"
               << " " << function_name << "_lpdf"
               << ", " << function_name << "_lpmf"
               << ", or " << function_name << "_log" << std::endl;
    return;
  }

  if ((internal_function_name.find("multiply_log") != std::string::npos) ||
      (internal_function_name.find("binomial_coefficient_log") !=
       std::string::npos)) {
    error_msgs << "Only distribution names can be used with"
               << " sampling (~) notation; found non-distribution"
               << " function: " << function_name << std::endl;
    pass = false;
    return;
  }

  if (internal_function_name.find("cdf_log") != std::string::npos) {
    error_msgs << "CDF and CCDF functions may not be used with"
               << " sampling notation."
               << " Use increment_log_prob(" << internal_function_name
               << "(...)) instead." << std::endl;
    pass = false;
    return;
  }

  if (internal_function_name == "lkj_cov_log") {
    error_msgs << "Info: the lkj_cov_log() sampling distribution"
               << " is deprecated.  It will be removed in Stan 3." << std::endl
               << "Code LKJ covariance in terms of an lkj_corr()"
               << " distribution on a correlation matrix"
               << " and independent lognormals on the scales." << std::endl
               << std::endl;
  }

  if (!is_double_return(internal_function_name, arg_types, error_msgs)) {
    error_msgs << "Real return type required for probability function."
               << std::endl;
    pass = false;
    return;
  }
  // test for LHS not being purely a variable
  if (has_non_param_var(s.expr_, var_map)) {
    error_msgs << "Info:" << std::endl
               << "Left-hand side of sampling statement (~) may contain a"
               << " non-linear transform of a parameter or local variable."
               << std::endl
               << "If it does, you need to include a target += statement"
               << " with the log absolute determinant of the Jacobian of"
               << " the transform." << std::endl
               << "Left-hand-side of sampling statement:" << std::endl
               << "    " << s.expr_.to_string() << " ~ " << function_name
               << "(...)" << std::endl;
  }
  // validate that variable and params are univariate if truncated
  if (s.truncation_.has_low() || s.truncation_.has_high()) {
    if (!is_univariate(s.expr_.bare_type())) {
      error_msgs << "Outcomes in truncated distributions"
                 << " must be univariate." << std::endl
                 << "  Found outcome expression: " << s.expr_.to_string()
                 << std::endl
                 << "  with non-univariate type: " << s.expr_.bare_type()
                 << std::endl;
      pass = false;
      return;
    }
    for (size_t i = 0; i < s.dist_.args_.size(); ++i)
      if (!is_univariate(s.dist_.args_[i].bare_type())) {
        error_msgs << "Parameters in truncated distributions"
                   << " must be univariate." << std::endl
                   << "  Found parameter expression: "
                   << s.dist_.args_[i].to_string() << std::endl
                   << "  with non-univariate type: "
                   << s.dist_.args_[i].bare_type() << std::endl;
        pass = false;
        return;
      }
  }
  if (s.truncation_.has_low() &&
      !is_univariate(s.truncation_.low_.bare_type())) {
    error_msgs << "Lower bounds in truncated distributions"
               << " must be univariate." << std::endl
               << "  Found lower bound expression: "
               << s.truncation_.low_.to_string() << std::endl
               << "  with non-univariate type: "
               << s.truncation_.low_.bare_type() << std::endl;
    pass = false;
    return;
  }
  if (s.truncation_.has_high() &&
      !is_univariate(s.truncation_.high_.bare_type())) {
    error_msgs << "Upper bounds in truncated distributions"
               << " must be univariate." << std::endl
               << "  Found upper bound expression: "
               << s.truncation_.high_.to_string() << std::endl
               << "  with non-univariate type: "
               << s.truncation_.high_.bare_type() << std::endl;
    pass = false;
    return;
  }

  // make sure CDFs or CCDFs exist with conforming signature
  // T[L, ]
  if (s.truncation_.has_low() && !s.truncation_.has_high()) {
    std::vector<bare_expr_type> arg_types_trunc(arg_types);
    arg_types_trunc[0] = s.truncation_.low_.bare_type();
    std::string function_name_ccdf = get_ccdf(s.dist_.family_);
    if (function_name_ccdf == s.dist_.family_ ||
        !is_double_return(function_name_ccdf, arg_types_trunc, error_msgs)) {
      error_msgs << "Lower truncation not defined for specified"
                 << " arguments to " << s.dist_.family_ << std::endl;
      pass = false;
      return;
    }
    if (!is_double_return(function_name_ccdf, arg_types, error_msgs)) {
      error_msgs << "Lower bound in truncation type does not match"
                 << " sampled variate in distribution's type" << std::endl;
      pass = false;
      return;
    }
  }
  // T[, H]
  if (!s.truncation_.has_low() && s.truncation_.has_high()) {
    std::vector<bare_expr_type> arg_types_trunc(arg_types);
    arg_types_trunc[0] = s.truncation_.high_.bare_type();
    std::string function_name_cdf = get_cdf(s.dist_.family_);
    if (function_name_cdf == s.dist_.family_ ||
        !is_double_return(function_name_cdf, arg_types_trunc, error_msgs)) {
      error_msgs << "Upper truncation not defined for"
                 << " specified arguments to " << s.dist_.family_ << std::endl;

      pass = false;
      return;
    }
    if (!is_double_return(function_name_cdf, arg_types, error_msgs)) {
      error_msgs << "Upper bound in truncation type does not match"
                 << " sampled variate in distribution's type" << std::endl;
      pass = false;
      return;
    }
  }
  // T[L, H]
  if (s.truncation_.has_low() && s.truncation_.has_high()) {
    std::vector<bare_expr_type> arg_types_trunc(arg_types);
    arg_types_trunc[0] = s.truncation_.low_.bare_type();
    std::string function_name_cdf = get_cdf(s.dist_.family_);
    if (function_name_cdf == s.dist_.family_ ||
        !is_double_return(function_name_cdf, arg_types_trunc, error_msgs)) {
      error_msgs << "Lower truncation not defined for specified"
                 << " arguments to " << s.dist_.family_ << std::endl;
      pass = false;
      return;
    }
    if (!is_double_return(function_name_cdf, arg_types, error_msgs)) {
      error_msgs << "Lower bound in truncation type does not match"
                 << " sampled variate in distribution's type" << std::endl;
      pass = false;
      return;
    }
  }
  pass = true;
}
boost::phoenix::function<validate_sample> validate_sample_f;

void expression_as_statement::operator()(bool &pass,
                                         const stan::lang::expression &expr,
                                         std::stringstream &error_msgs) const {
  if (!(expr.bare_type().is_void_type())) {
    error_msgs << "Illegal statement beginning with non-void"
               << " expression parsed as" << std::endl
               << "  " << expr.to_string() << std::endl
               << "Not a legal assignment, sampling, or function"
               << " statement.  Note that" << std::endl
               << "  * Assignment statements only allow variables"
               << " (with optional indexes) on the left;" << std::endl
               << "  * Sampling statements allow arbitrary"
               << " value-denoting expressions on the left." << std::endl
               << "  * Functions used as statements must be"
               << " declared to have void returns" << std::endl
               << std::endl;
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<expression_as_statement> expression_as_statement_f;

void unscope_locals::operator()(const std::vector<local_var_decl> &var_decls,
                                variable_map &vm) const {
  for (size_t i = 0; i < var_decls.size(); ++i)
    vm.remove(var_decls[i].name());
}
boost::phoenix::function<unscope_locals> unscope_locals_f;

void add_while_condition::operator()(while_statement &ws, const expression &e,
                                     bool &pass,
                                     std::stringstream &error_msgs) const {
  pass = e.bare_type().is_primitive();
  if (!pass) {
    error_msgs << "Conditions in while statement must be primitive"
               << " int or real;"
               << " found type=" << e.bare_type() << std::endl;
    return;
  }
  ws.condition_ = e;
}
boost::phoenix::function<add_while_condition> add_while_condition_f;

void add_while_body::operator()(while_statement &ws, const statement &s) const {
  ws.body_ = s;
}
boost::phoenix::function<add_while_body> add_while_body_f;

void add_loop_identifier::operator()(const std::string &name,
                                     const scope &var_scope,
                                     variable_map &vm) const {
  vm.add(name, var_decl(name, int_type()), scope(loop_identifier_origin, true));
}
boost::phoenix::function<add_loop_identifier> add_loop_identifier_f;

void add_array_loop_identifier ::operator()(const stan::lang::expression &expr,
                                            std::string &name,
                                            const scope &var_scope, bool &pass,
                                            variable_map &vm) const {
  pass = expr.bare_type().is_array_type();
  if (pass)
    vm.add(name, var_decl(name, expr.bare_type().array_element_type()),
           scope(loop_identifier_origin, true));
}
boost::phoenix::function<add_array_loop_identifier> add_array_loop_identifier_f;

void add_matrix_loop_identifier ::
operator()(const stan::lang::expression &expr, std::string &name,
           const scope &var_scope, bool &pass, variable_map &vm,
           std::stringstream &error_msgs) const {
  pass = expr.bare_type().num_dims() > 0 && !(expr.bare_type().is_array_type());
  if (!pass) {
    error_msgs << "Loop must be over container or range." << std::endl;
    return;
  } else {
    vm.add(name, var_decl(name, double_type()),
           scope(loop_identifier_origin, true));
    pass = true;
  }
}
boost::phoenix::function<add_matrix_loop_identifier>
    add_matrix_loop_identifier_f;

void store_loop_identifier::operator()(const std::string &name,
                                       std::string &name_local, bool &pass,
                                       variable_map &vm,
                                       std::stringstream &error_msgs) const {
  pass = !(vm.exists(name));
  if (!pass) {
    // avoid repeated error message due to backtracking
    if (error_msgs.str().find("Loop variable already declared.") ==
        std::string::npos)
      error_msgs << "Loop variable already declared."
                 << " variable name=\"" << name << "\"" << std::endl;
  } else {
    name_local = name;
  }
}
boost::phoenix::function<store_loop_identifier> store_loop_identifier_f;

void remove_loop_identifier::operator()(const std::string &name,
                                        variable_map &vm) const {
  vm.remove(name);
}
boost::phoenix::function<remove_loop_identifier> remove_loop_identifier_f;

void validate_int_expr::operator()(const expression &expr, bool &pass,
                                   std::stringstream &error_msgs) const {
  if (!expr.bare_type().is_int_type()) {
    error_msgs << "Expression denoting integer required; found type="
               << expr.bare_type() << std::endl;
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<validate_int_expr> validate_int_expr_f;

void deprecate_increment_log_prob::
operator()(std::stringstream &error_msgs) const {
  error_msgs << "Info: increment_log_prob(...);"
             << " is deprecated and will be removed in the future." << std::endl
             << "  Use target += ...; instead." << std::endl;
}
boost::phoenix::function<deprecate_increment_log_prob>
    deprecate_increment_log_prob_f;

void validate_allow_sample::operator()(const scope &var_scope, bool &pass,
                                       std::stringstream &error_msgs) const {
  pass = var_scope.allows_sampling();
  if (!pass)
    error_msgs << "Sampling statements (~) and increment_log_prob() are"
               << std::endl
               << "only allowed in the model block or lp functions."
               << std::endl;
}
boost::phoenix::function<validate_allow_sample> validate_allow_sample_f;

void validate_non_void_expression::operator()(const expression &e, bool &pass,
                                              std::ostream &error_msgs) const {
  pass = !e.bare_type().is_void_type();
  if (!pass)
    error_msgs << "Attempt to increment log prob with void expression"
               << std::endl;
}
boost::phoenix::function<validate_non_void_expression>
    validate_non_void_expression_f;

void add_literal_string::operator()(double_literal &lit,
                                    const pos_iterator_t &begin,
                                    const pos_iterator_t &end) const {
  lit.string_ = std::string(begin, end);
}
boost::phoenix::function<add_literal_string> add_literal_string_f;

template <typename T, typename I>
void add_line_number::operator()(T &line, const I &begin, const I &end) const {
  line.begin_line_ = get_line(begin);
  line.end_line_ = get_line(end);
}
boost::phoenix::function<add_line_number> add_line_number_f;

template void add_line_number::operator()(block_var_decl &,
                                          const pos_iterator_t &begin,
                                          const pos_iterator_t &end) const;

template void add_line_number::operator()(local_var_decl &,
                                          const pos_iterator_t &begin,
                                          const pos_iterator_t &end) const;

template void add_line_number::operator()(statement &,
                                          const pos_iterator_t &begin,
                                          const pos_iterator_t &end) const;

void set_void_return::operator()(return_statement &s) const {
  s = return_statement();
}
boost::phoenix::function<set_void_return> set_void_return_f;

void set_no_op::operator()(no_op_statement &s) const { s = no_op_statement(); }
boost::phoenix::function<set_no_op> set_no_op_f;

void deprecated_integrate_ode::operator()(std::ostream &error_msgs) const {
  error_msgs << "Info: the integrate_ode() function is deprecated"
             << " in the Stan language; use the following functions"
             << " instead.\n"
             << " integrate_ode_rk45()"
             << " [explicit, order 5, for non-stiff problems]\n"
             << " integrate_ode_adams()"
             << " [implicit, up to order 12, for non-stiff problems]\n"
             << " integrate_ode_bdf()"
             << " [implicit, up to order 5, for stiff problems]." << std::endl;
}
boost::phoenix::function<deprecated_integrate_ode> deprecated_integrate_ode_f;

template <class T>
void validate_integrate_ode_non_control_args(const T &ode_fun,
                                             const variable_map &var_map,
                                             bool &pass,
                                             std::ostream &error_msgs) {
  pass = true;

  // ode_integrator requires function with signature
  // (real, real[ ], real[ ], data real[ ], data int[ ]): real[ ]"

  // TODO(mitzi)  names indicate status, but not flagged as data_only
  // instantiate ode fn arg types
  double_type t_double;
  bare_expr_type t_ar_double(bare_array_type(t_double, 1));
  bare_expr_type t_ar_double_data(bare_array_type(t_double, 1));

  int_type t_int_data;
  bare_expr_type t_ar_int_data(bare_array_type(t_int_data, 1));

  // validate ode fn signature
  bare_expr_type sys_result_type(t_ar_double);
  std::vector<bare_expr_type> sys_arg_types;
  sys_arg_types.push_back(t_double);
  sys_arg_types.push_back(t_ar_double);
  sys_arg_types.push_back(t_ar_double);
  sys_arg_types.push_back(t_ar_double_data);
  sys_arg_types.push_back(t_ar_int_data);
  function_signature_t system_signature(sys_result_type, sys_arg_types);
  if (!function_signatures::instance().is_defined(ode_fun.system_function_name_,
                                                  system_signature)) {
    error_msgs << "Wrong signature for function "
               << ode_fun.integration_function_name_
               << "; first argument must be "
               << "the name of a function with signature"
               << " (real, real[ ], real[ ], data real[ ], data int[ ]):"
               << " real[ ]." << std::endl;
    pass = false;
  }

  // Stan lang integrate_ode takes 7 args:
  // fn_name, y0, t0, ts, theta, x_r, x_i
  // only y0 and theta can have params
  if (ode_fun.y0_.bare_type() != t_ar_double) {
    error_msgs << "Second argument to " << ode_fun.integration_function_name_
               << " must have type real[ ]"
               << "; found type = " << ode_fun.y0_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (!ode_fun.t0_.bare_type().is_primitive()) {
    error_msgs << "Third argument to " << ode_fun.integration_function_name_
               << " must have type real"
               << ";  found type = " << ode_fun.t0_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (ode_fun.ts_.bare_type() != t_ar_double) {
    error_msgs << "Fourth argument to " << ode_fun.integration_function_name_
               << " must have type real[ ]"
               << ";  found type = " << ode_fun.ts_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (ode_fun.theta_.bare_type() != t_ar_double) {
    error_msgs << "Fifth argument to " << ode_fun.integration_function_name_
               << " must have type real[ ]"
               << ";  found type = " << ode_fun.theta_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (ode_fun.x_.bare_type() != t_ar_double_data) {
    error_msgs << "Sixth argument to " << ode_fun.integration_function_name_
               << " must have type data real[ ]"
               << ";  found type = " << ode_fun.x_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (ode_fun.x_int_.bare_type() != t_ar_int_data) {
    error_msgs << "Seventh argument to " << ode_fun.integration_function_name_
               << " must have type data int[ ]"
               << ";  found type = " << ode_fun.x_int_.bare_type() << ". "
               << std::endl;
    pass = false;
  }

  // test data-only variables do not have parameters (int locals OK)
  if (has_var(ode_fun.t0_, var_map)) {
    error_msgs << "Third argument to " << ode_fun.integration_function_name_
               << " (initial times)"
               << " must be data only and not reference parameters."
               << std::endl;
    pass = false;
  }
  if (has_var(ode_fun.ts_, var_map)) {
    error_msgs << "Fourth argument to " << ode_fun.integration_function_name_
               << " (solution times)"
               << " must be data only and not reference parameters."
               << std::endl;
    pass = false;
  }
  if (has_var(ode_fun.x_, var_map)) {
    error_msgs << "Sixth argument to " << ode_fun.integration_function_name_
               << " (real data)"
               << " must be data only and not reference parameters."
               << std::endl;
    pass = false;
  }
}

void validate_integrate_ode::operator()(const integrate_ode &ode_fun,
                                        const variable_map &var_map, bool &pass,
                                        std::ostream &error_msgs) const {
  validate_integrate_ode_non_control_args(ode_fun, var_map, pass, error_msgs);
}
boost::phoenix::function<validate_integrate_ode> validate_integrate_ode_f;

void validate_integrate_ode_control::
operator()(const integrate_ode_control &ode_fun, const variable_map &var_map,
           bool &pass, std::ostream &error_msgs) const {
  validate_integrate_ode_non_control_args(ode_fun, var_map, pass, error_msgs);
  if (!ode_fun.rel_tol_.bare_type().is_primitive()) {
    error_msgs << "Eighth argument to " << ode_fun.integration_function_name_
               << " (relative tolerance) must have type real or int;"
               << " found type=" << ode_fun.rel_tol_.bare_type() << ". ";
    pass = false;
  }
  if (!ode_fun.abs_tol_.bare_type().is_primitive()) {
    error_msgs << "Ninth argument to " << ode_fun.integration_function_name_
               << " (absolute tolerance) must have type real or int;"
               << " found type=" << ode_fun.abs_tol_.bare_type() << ". ";
    pass = false;
  }
  if (!ode_fun.max_num_steps_.bare_type().is_primitive()) {
    error_msgs << "Tenth argument to " << ode_fun.integration_function_name_
               << " (max steps) must have type real or int;"
               << " found type=" << ode_fun.max_num_steps_.bare_type() << ". ";
    pass = false;
  }

  // test data-only variables do not have parameters (int locals OK)
  if (has_var(ode_fun.rel_tol_, var_map)) {
    error_msgs << "Eighth argument to " << ode_fun.integration_function_name_
               << " (relative tolerance) must be data only"
               << " and not depend on parameters.";
    pass = false;
  }
  if (has_var(ode_fun.abs_tol_, var_map)) {
    error_msgs << "Ninth argument to " << ode_fun.integration_function_name_
               << " (absolute tolerance ) must be data only"
               << " and not depend parameters.";
    pass = false;
  }
  if (has_var(ode_fun.max_num_steps_, var_map)) {
    error_msgs << "Tenth argument to " << ode_fun.integration_function_name_
               << " (max steps) must be data only"
               << " and not depend on parameters.";
    pass = false;
  }
}
boost::phoenix::function<validate_integrate_ode_control>
    validate_integrate_ode_control_f;

template <class T>
void validate_algebra_solver_non_control_args(const T &alg_fun,
                                              const variable_map &var_map,
                                              bool &pass,
                                              std::ostream &error_msgs) {
  pass = true;
  int_type t_int;
  double_type t_double;
  vector_type t_vector;
  bare_expr_type t_ar_int(bare_array_type(t_int, 1));
  bare_expr_type t_ar_double(bare_array_type(t_double, 1));

  bare_expr_type sys_result_type(t_vector);
  std::vector<bare_expr_type> sys_arg_types;
  sys_arg_types.push_back(t_vector);    // y
  sys_arg_types.push_back(t_vector);    // theta
  sys_arg_types.push_back(t_ar_double);  // x_r
  sys_arg_types.push_back(t_ar_int);    // x_i
  function_signature_t system_signature(sys_result_type, sys_arg_types);

  if (!function_signatures::instance().is_defined(alg_fun.system_function_name_,
                                                  system_signature)) {
    error_msgs << "Wrong signature for function "
               << alg_fun.system_function_name_
               << "; first argument to algebra_solver"
               << " must be a function with signature"
               << " (vector, vector, real[ ], int[ ]) : vector." << std::endl;
    pass = false;
  }

  // check solver function arg types
  if (alg_fun.y_.bare_type() != t_vector) {
    error_msgs << "Second argument to algebra_solver must have type vector"
               << "; found type= " << alg_fun.y_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (alg_fun.theta_.bare_type() != t_vector) {
    error_msgs << "Third argument to algebra_solver must have type vector"
               << ";  found type= " << alg_fun.theta_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (alg_fun.x_r_.bare_type() != t_ar_double) {
    error_msgs << "Fourth argument to algebra_solver must have type real[ ]"
               << ";  found type= " << alg_fun.x_r_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (alg_fun.x_i_.bare_type() != t_ar_int) {
    error_msgs << "Fifth argument to algebra_solver must have type int[ ]"
               << ";  found type= " << alg_fun.x_i_.bare_type() << ". "
               << std::endl;
    pass = false;
  }

  // real data array cannot be param or x-formed param variable
  if (has_var(alg_fun.x_r_, var_map)) {
    error_msgs << "Fourth argument to algebra_solver"
               << " must be data only (cannot reference parameters)."
               << std::endl;
    pass = false;
  }
}

void validate_algebra_solver::operator()(const algebra_solver &alg_fun,
                                         const variable_map &var_map,
                                         bool &pass,
                                         std::ostream &error_msgs) const {
  validate_algebra_solver_non_control_args(alg_fun, var_map, pass, error_msgs);
}
boost::phoenix::function<validate_algebra_solver> validate_algebra_solver_f;

void validate_algebra_solver_control::
operator()(const algebra_solver_control &alg_fun, const variable_map &var_map,
           bool &pass, std::ostream &error_msgs) const {
  validate_algebra_solver_non_control_args(alg_fun, var_map, pass, error_msgs);
  if (!alg_fun.rel_tol_.bare_type().is_primitive()) {
    error_msgs << "Sixth argument to algebra_solver "
               << " (relative tolerance) must have type real or int;"
               << " found type=" << alg_fun.rel_tol_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (!alg_fun.fun_tol_.bare_type().is_primitive()) {
    error_msgs << "Seventh argument to algebra_solver "
               << " (function tolerance) must have type real or int;"
               << " found type=" << alg_fun.fun_tol_.bare_type() << ". "
               << std::endl;
    pass = false;
  }
  if (!alg_fun.max_num_steps_.bare_type().is_primitive()) {
    error_msgs << "Eighth argument to algebra_solver"
               << " (max number of steps) must have type real or int;"
               << " found type=" << alg_fun.max_num_steps_.bare_type() << ". "
               << std::endl;
    pass = false;
  }

  // control args cannot contain param variables
  if (has_var(alg_fun.rel_tol_, var_map)) {
    error_msgs << "Sixth argument to algebra_solver"
               << " (relative tolerance) must be data only"
               << " and not depend on parameters" << std::endl;
    pass = false;
  }
  if (has_var(alg_fun.fun_tol_, var_map)) {
    error_msgs << "Seventh argument to algebra_solver"
               << " (function tolerance ) must be data only"
               << " and not depend parameters" << std::endl;
    pass = false;
  }
  if (has_var(alg_fun.max_num_steps_, var_map)) {
    error_msgs << "Eighth argument to algebra_solver"
               << " (max number of steps) must be data only"
               << " and not depend on parameters" << std::endl;
    pass = false;
  }
}
boost::phoenix::function<validate_algebra_solver_control>
    validate_algebra_solver_control_f;

void validate_integrate_1d::operator()(integrate_1d &fx,
                                       const variable_map &var_map, bool &pass,
                                       std::ostream &error_msgs) const {
  pass = true;

  // (1) name of function to integrate
  if (ends_with("_rng", fx.function_name_)) {
    error_msgs << "integrated function may not be an _rng function,"
               << " found function name: " << fx.function_name_ << std::endl;
    pass = false;
  }
  double_type t_double;
  bare_expr_type sys_result_type(t_double);
  std::vector<bare_expr_type> sys_arg_types;
  sys_arg_types.push_back(bare_expr_type(t_double));
  sys_arg_types.push_back(bare_expr_type(t_double));
  sys_arg_types.push_back(bare_expr_type(bare_array_type(double_type(), 1)));
  sys_arg_types.push_back(bare_expr_type(bare_array_type(double_type(), 1)));
  sys_arg_types.push_back(bare_expr_type(bare_array_type(int_type(), 1)));
  function_signature_t system_signature(sys_result_type, sys_arg_types);
  if (!function_signatures::instance().is_defined(fx.function_name_,
                                                  system_signature)) {
    pass = false;
    error_msgs << "first argument to integrate_1d"
               << " must be the name of a function with signature"
               << " (real, real, real[], real[], int[]) : real " << std::endl;
  }

  // (2) lower bound of integration
  if (!fx.lb_.bare_type().is_primitive()) {
    pass = false;
    error_msgs << "second argument to integrate_1d, the lower bound of"
               << " integration, must have type int or real;"
               << " found type = " << fx.lb_.bare_type() << "." << std::endl;
  }

  // (3) lower bound of integration
  if (!fx.ub_.bare_type().is_primitive()) {
    pass = false;
    error_msgs << "third argument to integrate_1d, the upper bound of"
               << " integration, must have type int or real;"
               << " found type = " << fx.ub_.bare_type() << "." << std::endl;
  }

  // (4) parameters
  if (fx.theta_.bare_type() != bare_array_type(double_type(), 1)) {
    pass = false;
    error_msgs << "fourth argument to integrate_1d, the parameters,"
               << " must have type real[];"
               << " found type = " << fx.theta_.bare_type() << "." << std::endl;
  }

  // (5) real data
  if (fx.x_r_.bare_type() != bare_array_type(double_type(), 1)) {
    pass = false;
    error_msgs << "fifth argument to integrate_1d, the real data,"
               << " must have type real[]; found type = " << fx.x_r_.bare_type()
               << "." << std::endl;
  }

  // (6) int data
  if (fx.x_i_.bare_type() != bare_array_type(int_type(), 1)) {
    pass = false;
    error_msgs << "sixth argument to integrate_1d, the integer data,"
               << " must have type int[]; found type = " << fx.x_i_.bare_type()
               << "." << std::endl;
  }

  // (7) relative tolerance
  if (!fx.rel_tol_.bare_type().is_primitive()) {
    pass = false;
    error_msgs << "seventh argument to integrate_1d, relative tolerance,"
               << " must be of type int or real;  found type = "
               << fx.rel_tol_.bare_type() << "." << std::endl;
  }

  // test data-only variables do not have parameters (int locals OK)
  if (has_var(fx.x_r_, var_map)) {
    pass = false;
    error_msgs << "fifth argument to integrate_1d, the real data,"
               << " must be data only and not reference parameters."
               << std::endl;
  }
  if (has_var(fx.rel_tol_, var_map)) {
    pass = false;
    error_msgs << "seventh argument to integrate_1d, relative tolerance,"
               << " must be data only and not reference parameters."
               << std::endl;
  }
}
boost::phoenix::function<validate_integrate_1d> validate_integrate_1d_f;
void validate_map_rect::operator()(map_rect &mr, const variable_map &var_map,
                                   bool &pass, std::ostream &error_msgs) const {
  pass = true;

  if (has_rng_lp_suffix(mr.fun_name_)) {
    error_msgs << "Mapped function cannot be an _rng or _lp function,"
               << " found function name: " << mr.fun_name_ << std::endl;
    pass = false;
  }

  // mapped function signature
  // vector f(vector param_shared, vector param_local,
  //          real[] data_r, int[] data_i)
  int_type t_int;
  double_type t_double;
  vector_type t_vector;
  bare_expr_type t_ar_int(bare_array_type(t_int, 1));
  bare_expr_type t_2d_ar_int(bare_array_type(t_int, 2));
  bare_expr_type t_ar_double(bare_array_type(t_double, 1));
  bare_expr_type t_2d_ar_double(bare_array_type(t_double, 2));
  bare_expr_type t_ar_vector(bare_array_type(t_vector, 1));
  bare_expr_type shared_params_type(t_vector);
  bare_expr_type job_params_type(t_vector);
  bare_expr_type job_data_r_type(t_ar_double);
  bare_expr_type job_data_i_type(t_ar_int);
  bare_expr_type result_type(t_vector);
  std::vector<bare_expr_type> arg_types;
  arg_types.push_back(shared_params_type);
  arg_types.push_back(job_params_type);
  arg_types.push_back(job_data_r_type);
  arg_types.push_back(job_data_i_type);
  function_signature_t mapped_fun_signature(result_type, arg_types);

  // validate mapped function signature
  if (!function_signatures::instance().is_defined(mr.fun_name_,
                                                  mapped_fun_signature)) {
    error_msgs << "First argument to map_rect"
               << " must be the name of a function with signature"
               << " (vector, vector, real[ ], int[ ]) : vector." << std::endl;
    pass = false;
  }

  // shared parameters - vector
  if (mr.shared_params_.bare_type() != shared_params_type) {
    if (!pass)
      error_msgs << ";  ";
    error_msgs << "Second argument to map_rect must be of type vector."
               << std::endl;
    pass = false;
  }
  // job-specific parameters - array of vectors (array elts map to arg2)
  if (mr.job_params_.bare_type() != t_ar_vector) {
    if (!pass)
      error_msgs << ";  ";
    error_msgs << "Third argument to map_rect must be of type vector[ ]"
               << " (array of vectors)." << std::endl;
    pass = false;
  }
  // job-specific real data - 2-d array of double (array elts map to arg3)
  bare_expr_type job_data_rs_type(t_2d_ar_double);
  if (mr.job_data_r_.bare_type() != job_data_rs_type) {
    if (!pass)
      error_msgs << ";  ";
    error_msgs << "Fourth argument to map_rect must be of type real[ , ]"
               << " (two dimensional array of reals)." << std::endl;
    pass = false;
  }
  // job-specific int data - 2-d array of int (array elts map to arg4)
  bare_expr_type job_data_is_type(t_2d_ar_int);
  if (mr.job_data_i_.bare_type() != job_data_is_type) {
    if (!pass)
      error_msgs << ";  ";
    error_msgs << "Fifth argument to map_rect must be of type int[ , ]"
               << " (two dimensional array of integers)." << std::endl;
    pass = false;
  }

  // test data is data only
  if (has_var(mr.job_data_r_, var_map)) {
    if (!pass)
      error_msgs << ";  ";
    error_msgs << "Fourth argment to map_rect must be data only." << std::endl;
    pass = false;
  }
  if (has_var(mr.job_data_i_, var_map)) {
    if (!pass)
      error_msgs << ";  ";
    error_msgs << "Fifth argument to map_rect must be data only." << std::endl;
    pass = false;
  }

  if (pass)
    mr.register_id();
}
boost::phoenix::function<validate_map_rect> validate_map_rect_f;

void set_fun_type_named::operator()(expression &fun_result, fun &fun,
                                    const scope &var_scope, bool &pass,
                                    const variable_map &var_map,
                                    std::ostream &error_msgs) const {
  if (fun.name_ == "get_lp")
    error_msgs << "Info: get_lp() function deprecated." << std::endl
               << "  It will be removed in a future release." << std::endl
               << "  Use target() instead." << std::endl;
  if (fun.name_ == "target")
    fun.name_ = "get_lp";  // for code gen and context validation

  std::vector<bare_expr_type> arg_types;
  for (size_t i = 0; i < fun.args_.size(); ++i)
    arg_types.push_back(fun.args_[i].bare_type());

  fun.type_ = function_signatures::instance().get_result_type(
      fun.name_, arg_types, error_msgs);
  if (fun.type_.is_ill_formed_type()) {
    pass = false;
    return;
  }

  // get function definition for this functiion
  std::vector<bare_expr_type> fun_arg_types;
  for (size_t i = 0; i < fun.args_.size(); ++i)
    fun_arg_types.push_back(arg_types[i]);
  function_signature_t sig(fun.type_, fun_arg_types);
  function_signature_t decl_sig =
      function_signatures::instance().get_definition(fun.name_, sig);
  if (!decl_sig.first.is_ill_formed_type()) {
    for (size_t i = 0; i < fun_arg_types.size(); ++i) {
      if (decl_sig.second[i].is_data() && has_var(fun.args_[i], var_map)) {
        error_msgs << "Function argument error, function: " << fun.name_
                   << ", argument: " << (i + 1) << " must be data only, "
                   << "found expression containing a parameter varaible."
                   << std::endl;
        pass = false;
        return;
      }
    }
  }

  // disjunction so only first match triggered
  deprecate_fun("binomial_coefficient_log", "lchoose", fun, error_msgs) ||
      deprecate_fun("multiply_log", "lmultiply", fun, error_msgs) ||
      deprecate_suffix("_cdf_log", "'_lcdf'", fun, error_msgs) ||
      deprecate_suffix("_ccdf_log", "'_lccdf'", fun, error_msgs) ||
      deprecate_suffix(
          "_log", "'_lpdf' for density functions or '_lpmf' for mass functions",
          fun, error_msgs);

  // use old function names for built-in prob funs
  if (!function_signatures::instance().has_user_defined_key(fun.name_)) {
    replace_suffix("_lpdf", "_log", fun);
    replace_suffix("_lpmf", "_log", fun);
    replace_suffix("_lcdf", "_cdf_log", fun);
    replace_suffix("_lccdf", "_ccdf_log", fun);
  }
  // know these are not user-defined`x
  replace_suffix("lmultiply", "multiply_log", fun);
  replace_suffix("lchoose", "binomial_coefficient_log", fun);

  if (has_rng_suffix(fun.name_)) {
    if (!(var_scope.allows_rng())) {
      error_msgs << "Random number generators only allowed in"
                 << " transformed data block, generated quantities block"
                 << " or user-defined functions with names ending in _rng"
                 << "; found function=" << fun.name_ << " in block=";
      print_scope(error_msgs, var_scope);
      error_msgs << std::endl;
      pass = false;
      return;
    }
  }

  if (has_lp_suffix(fun.name_) || fun.name_ == "target") {
    if (!(var_scope.allows_lp_fun())) {
      error_msgs << "Function target() or functions suffixed with _lp only"
                 << " allowed in transformed parameter block, model block"
                 << std::endl
                 << "or the body of a function with suffix _lp." << std::endl
                 << "Found function = "
                 << (fun.name_ == "get_lp" ? "target or get_lp" : fun.name_)
                 << " in block = ";
      print_scope(error_msgs, var_scope);
      error_msgs << std::endl;
      pass = false;
      return;
    }
  }

  if (fun.name_ == "abs" && fun.args_.size() > 0 &&
      fun.args_[0].bare_type().is_double_type()) {
    error_msgs << "Info: Function abs(real) is deprecated"
               << " in the Stan language." << std::endl
               << "         It will be removed in a future release."
               << std::endl
               << "         Use fabs(real) instead." << std::endl
               << std::endl;
  }

  if (fun.name_ == "lkj_cov_log") {
    error_msgs << "Info: the lkj_cov_log() function"
               << " is deprecated.  It will be removed in Stan 3." << std::endl
               << "Code LKJ covariance in terms of an lkj_corr()"
               << " distribution on a correlation matrix"
               << " and independent lognormals on the scales." << std::endl
               << std::endl;
  }

  if (fun.name_ == "if_else") {
    error_msgs << "Info: the if_else() function"
               << " is deprecated.  "
               << "Use the conditional operator '?:' instead." << std::endl;
  }

  // add namespace qualifier to avoid ambiguities w/ c math fns
  qualify_builtins(fun);

  fun_result = fun;
  pass = true;
}
boost::phoenix::function<set_fun_type_named> set_fun_type_named_f;

void infer_array_expr_type::operator()(expression &e, array_expr &array_expr,
                                       const scope &var_scope, bool &pass,
                                       const variable_map &var_map,
                                       std::ostream &error_msgs) const {
  if (array_expr.args_.size() == 0) {
    // shouldn't occur, because of % operator used to construct it
    error_msgs << "Array expression found size 0, must be > 0";
    array_expr.type_ = ill_formed_type();
    pass = false;
    return;
  }
  bare_expr_type e_first(array_expr.args_[0].bare_type());
  for (size_t i = 1; i < array_expr.args_.size(); ++i) {
    bare_expr_type e_next(array_expr.args_[i].bare_type());
    if (e_first != e_next) {
      if (e_first.is_primitive() && e_next.is_primitive()) {
        e_first = double_type();
      } else {
        error_msgs << "Expressions for elements of array must have"
                   << " the same or promotable types; found"
                   << " first element type=" << e_first << "; type at position "
                   << i + 1 << "=" << e_next;
        array_expr.type_ = ill_formed_type();
        pass = false;
        return;
      }
    }
  }
  array_expr.type_ = bare_array_type(e_first);
  array_expr.array_expr_scope_ = var_scope;
  array_expr.has_var_ = has_var(array_expr, var_map);
  e = array_expr;
  pass = true;
}
boost::phoenix::function<infer_array_expr_type> infer_array_expr_type_f;

void infer_vec_or_matrix_expr_type::
operator()(expression &e, row_vector_expr &vec_expr, const scope &var_scope,
           bool &pass, const variable_map &var_map,
           std::ostream &error_msgs) const {
  if (vec_expr.args_.size() == 0) {
    // shouldn't occur, because of % operator used to construct it
    error_msgs << "Vector or matrix expression found size 0, must be > 0";
    pass = false;
    return;
  }
  bare_expr_type e_first = vec_expr.args_[0].bare_type();
  if (!(e_first.is_primitive() || e_first.is_row_vector_type())) {
    error_msgs << "Matrix expression elements must be type row_vector "
               << "and row vector expression elements must be int "
               << "or real, but found element of type " << e_first << std::endl;
    pass = false;
    return;
  }
  bool is_matrix_el = e_first.is_row_vector_type();
  for (size_t i = 1; i < vec_expr.args_.size(); ++i) {
    if (is_matrix_el && !vec_expr.args_[i].bare_type().is_row_vector_type()) {
      error_msgs << "Matrix expression elements must be type row_vector, "
                 << "but found element of type "
                 << vec_expr.args_[i].bare_type() << std::endl;
      pass = false;
      return;
    } else if (!is_matrix_el &&
               !(vec_expr.args_[i].bare_type().is_primitive())) {
      error_msgs << "Row vector expression elements must be int or real, "
                 << "but found element of type "
                 << vec_expr.args_[i].bare_type() << std::endl;
      pass = false;
      return;
    }
  }
  if (is_matrix_el) {
    // create matrix expr object
    matrix_expr me = matrix_expr(vec_expr.args_);
    me.matrix_expr_scope_ = var_scope;
    me.has_var_ = has_var(me, var_map);
    e = me;
  } else {
    vec_expr.row_vector_expr_scope_ = var_scope;
    vec_expr.has_var_ = has_var(vec_expr, var_map);
    e = vec_expr;
  }
  pass = true;
}
boost::phoenix::function<infer_vec_or_matrix_expr_type>
    infer_vec_or_matrix_expr_type_f;

void exponentiation_expr::operator()(expression &expr1, const expression &expr2,
                                     const scope &var_scope, bool &pass,
                                     std::ostream &error_msgs) const {
  if (!expr1.bare_type().is_primitive() || !expr2.bare_type().is_primitive()) {
    error_msgs << "Arguments to ^ must be primitive (real or int)"
               << "; cannot exponentiate " << expr1.bare_type() << " by "
               << expr2.bare_type() << " in block=";
    print_scope(error_msgs, var_scope);
    error_msgs << std::endl;
    pass = false;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("pow", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<exponentiation_expr> exponentiation_f;

void multiplication_expr::operator()(expression &expr1, const expression &expr2,
                                     std::ostream &error_msgs) const {
  if (expr1.bare_type().is_primitive() && expr2.bare_type().is_primitive()) {
    expr1 *= expr2;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("multiply", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<multiplication_expr> multiplication_f;

void division_expr::operator()(expression &expr1, const expression &expr2,
                               std::ostream &error_msgs) const {
  if (expr1.bare_type().is_primitive() && expr2.bare_type().is_primitive() &&
      (expr1.bare_type().is_double_type() ||
       expr2.bare_type().is_double_type())) {
    expr1 /= expr2;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);

  if (expr1.bare_type().is_int_type() && expr2.bare_type().is_int_type()) {
    // result might be assigned to real - generate warning
    error_msgs << "Info: integer division"
               << " implicitly rounds to integer."
               << " Found int division: " << expr1.to_string() << " / "
               << expr2.to_string() << std::endl
               << " Positive values rounded down,"
               << " negative values rounded up or down"
               << " in platform-dependent way." << std::endl;

    fun f("divide", args);
    set_fun_type(f, error_msgs);
    expr1 = expression(f);
    return;
  }
  if ((expr1.bare_type().is_matrix_type() ||
       expr1.bare_type().is_row_vector_type()) &&
      expr2.bare_type().is_matrix_type()) {
    fun f("mdivide_right", args);
    set_fun_type(f, error_msgs);
    expr1 = expression(f);
    return;
  }
  fun f("divide", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
  return;
}
boost::phoenix::function<division_expr> division_f;

void modulus_expr::operator()(expression &expr1, const expression &expr2,
                              bool &pass, std::ostream &error_msgs) const {
  if (!expr1.bare_type().is_int_type() && !expr2.bare_type().is_int_type()) {
    error_msgs << "Both operands of % must be int"
               << "; cannot modulo " << expr1.bare_type() << " by "
               << expr2.bare_type();
    error_msgs << std::endl;
    pass = false;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("modulus", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<modulus_expr> modulus_f;

void left_division_expr::operator()(expression &expr1, bool &pass,
                                    const expression &expr2,
                                    std::ostream &error_msgs) const {
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  if (expr1.bare_type().is_matrix_type() &&
      (expr2.bare_type().is_vector_type() ||
       expr2.bare_type().is_matrix_type())) {
    fun f("mdivide_left", args);
    set_fun_type(f, error_msgs);
    expr1 = expression(f);
    pass = true;
    return;
  }
  fun f("mdivide_left", args);  // set for alt args err msg
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
  pass = false;
}
boost::phoenix::function<left_division_expr> left_division_f;

void elt_multiplication_expr::operator()(expression &expr1,
                                         const expression &expr2,
                                         std::ostream &error_msgs) const {
  if (expr1.bare_type().is_primitive() && expr2.bare_type().is_primitive()) {
    expr1 *= expr2;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("elt_multiply", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<elt_multiplication_expr> elt_multiplication_f;

void elt_division_expr::operator()(expression &expr1, const expression &expr2,
                                   std::ostream &error_msgs) const {
  if (expr1.bare_type().is_primitive() && expr2.bare_type().is_primitive()) {
    expr1 /= expr2;
    return;
  }
  std::vector<expression> args;
  args.push_back(expr1);
  args.push_back(expr2);
  fun f("elt_divide", args);
  set_fun_type(f, error_msgs);
  expr1 = expression(f);
}
boost::phoenix::function<elt_division_expr> elt_division_f;

void negate_expr::operator()(expression &expr_result, const expression &expr,
                             bool &pass, std::ostream &error_msgs) const {
  if (expr.bare_type().is_primitive()) {
    expr_result = expression(unary_op('-', expr));
    return;
  }
  std::vector<expression> args;
  args.push_back(expr);
  fun f("minus", args);
  set_fun_type(f, error_msgs);
  expr_result = expression(f);
}
boost::phoenix::function<negate_expr> negate_expr_f;

void logical_negate_expr::operator()(expression &expr_result,
                                     const expression &expr,
                                     std::ostream &error_msgs) const {
  if (!expr.bare_type().is_primitive()) {
    error_msgs << "Logical negation operator !"
               << " only applies to int or real types; ";
    expr_result = expression();
  }
  std::vector<expression> args;
  args.push_back(expr);
  fun f("logical_negation", args);
  set_fun_type(f, error_msgs);
  expr_result = expression(f);
}
boost::phoenix::function<logical_negate_expr> logical_negate_expr_f;

void transpose_expr::operator()(expression &expr, bool &pass,
                                std::ostream &error_msgs) const {
  if (expr.bare_type().is_primitive())
    return;
  std::vector<expression> args;
  args.push_back(expr);
  fun f("transpose", args);
  set_fun_type(f, error_msgs);
  expr = expression(f);
  pass = !expr.bare_type().is_ill_formed_type();
}
boost::phoenix::function<transpose_expr> transpose_f;

void add_idxs::operator()(expression &e, std::vector<idx> &idxs, bool &pass,
                          std::ostream &error_msgs) const {
  e = index_op_sliced(e, idxs);
  pass = !e.bare_type().is_ill_formed_type();
  if (!pass)
    error_msgs << "Indexed expression must have at least as many"
               << " dimensions as number of indexes supplied:" << std::endl
               << " indexed expression dims=" << e.total_dims()
               << "; num indexes=" << idxs.size() << std::endl;
}
boost::phoenix::function<add_idxs> add_idxs_f;

void add_expression_dimss::
operator()(expression &expression,
           std::vector<std::vector<stan::lang::expression>> &dimss, bool &pass,
           std::ostream &error_msgs) const {
  int expr_dims = expression.total_dims();
  int index_dims = num_dimss(dimss);
  if (expr_dims < index_dims) {
    error_msgs << "Too many indexes, expression dimensions=" << expr_dims
               << ", indexes found=" << index_dims << std::endl;
    pass = false;
    return;
  }
  index_op iop(expression, dimss);
  iop.infer_type();
  if (iop.type_.is_ill_formed_type()) {
    error_msgs << "Indexed expression must have at least as many"
               << " dimensions as number of indexes supplied." << std::endl;
    pass = false;
    return;
  }
  pass = true;
  expression = iop;
}
boost::phoenix::function<add_expression_dimss> add_expression_dimss_f;

void set_var_type::operator()(variable &var_expr, expression &val,
                              variable_map &vm, std::ostream &error_msgs,
                              bool &pass) const {
  std::string name = var_expr.name_;
  if (name == std::string("lp__")) {
    error_msgs << std::endl
               << "Error (fatal):  Use of lp__ is no longer supported."
               << std::endl
               << "  Use target += ... statement to increment log density."
               << std::endl
               << "  Use target() function to get log density." << std::endl;
    pass = false;
    return;
  } else if (name == std::string("params_r__")) {
    error_msgs << std::endl
               << "Info:" << std::endl
               << "  Direct access to params_r__ yields an inconsistent"
               << " statistical model in isolation and no guarantee is"
               << " made that this model will yield valid inferences."
               << std::endl
               << "  Moreover, access to params_r__ is unsupported"
               << " and the variable may be removed without notice."
               << std::endl;
  } else if (name == std::string("data") || name == std::string("generated") ||
             name == std::string("model") ||
             name == std::string("parameters") ||
             name == std::string("transformed")) {
    error_msgs << std::endl
               << "Unexpected open block, missing close block \"}\""
               << " before keyword \"" << name << "\"." << std::endl;
    pass = false;
    return;
  }
  pass = vm.exists(name);
  if (pass) {
    var_expr.set_type(vm.get_bare_type(name));
  } else {
    error_msgs << "Variable \"" << name << '"' << " does not exist."
               << std::endl;
    return;
  }
  val = expression(var_expr);
}
boost::phoenix::function<set_var_type> set_var_type_f;

void require_vbar::operator()(bool &pass, std::ostream &error_msgs) const {
  pass = false;
  error_msgs << "Probabilty functions with suffixes _lpdf, _lpmf,"
             << " _lcdf, and _lccdf," << std::endl
             << "require a vertical bar (|) between the first two"
             << " arguments." << std::endl;
}
boost::phoenix::function<require_vbar> require_vbar_f;

data_only_expression::data_only_expression(std::stringstream &error_msgs,
                                           variable_map &var_map)
    : error_msgs_(error_msgs), var_map_(var_map) {}
bool data_only_expression::operator()(const nil & /*e*/) const { return true; }
bool data_only_expression::operator()(const int_literal & /*x*/) const {
  return true;
}
bool data_only_expression::operator()(const double_literal & /*x*/) const {
  return true;
}
bool data_only_expression::operator()(const array_expr &x) const {
  for (size_t i = 0; i < x.args_.size(); ++i)
    if (!boost::apply_visitor(*this, x.args_[i].expr_))
      return false;
  return true;
}
bool data_only_expression::operator()(const matrix_expr &x) const {
  for (size_t i = 0; i < x.args_.size(); ++i)
    if (!boost::apply_visitor(*this, x.args_[i].expr_))
      return false;
  return true;
}
bool data_only_expression::operator()(const row_vector_expr &x) const {
  for (size_t i = 0; i < x.args_.size(); ++i)
    if (!boost::apply_visitor(*this, x.args_[i].expr_))
      return false;
  return true;
}
bool data_only_expression::operator()(const variable &x) const {
  scope var_scope = var_map_.get_scope(x.name_);
  bool is_data = var_scope.allows_size();
  if (!is_data) {
    error_msgs_ << "Non-data variables are not allowed"
                << " in dimension declarations;"
                << " found variable=" << x.name_ << "; declared in block=";
    print_scope(error_msgs_, var_scope);
    error_msgs_ << "." << std::endl;
  }
  return is_data;
}
bool data_only_expression::operator()(const integrate_1d &x) const {
  return boost::apply_visitor(*this, x.lb_.expr_) &&
         boost::apply_visitor(*this, x.ub_.expr_) &&
         boost::apply_visitor(*this, x.theta_.expr_);
}
bool data_only_expression::operator()(const integrate_ode &x) const {
  return boost::apply_visitor(*this, x.y0_.expr_) &&
         boost::apply_visitor(*this, x.theta_.expr_);
}
bool data_only_expression::operator()(const integrate_ode_control &x) const {
  return boost::apply_visitor(*this, x.y0_.expr_) &&
         boost::apply_visitor(*this, x.theta_.expr_);
}
bool data_only_expression::operator()(const algebra_solver &x) const {
  return boost::apply_visitor(*this, x.theta_.expr_);
}
bool data_only_expression::operator()(const algebra_solver_control &x) const {
  return boost::apply_visitor(*this, x.theta_.expr_);
}
bool data_only_expression::operator()(const map_rect &x) const {
  return boost::apply_visitor(*this, x.shared_params_.expr_) &&
         boost::apply_visitor(*this, x.job_params_.expr_);
}
bool data_only_expression::operator()(const fun &x) const {
  for (size_t i = 0; i < x.args_.size(); ++i)
    if (!boost::apply_visitor(*this, x.args_[i].expr_))
      return false;
  return true;
}
bool data_only_expression::operator()(const index_op &x) const {
  if (!boost::apply_visitor(*this, x.expr_.expr_))
    return false;
  for (size_t i = 0; i < x.dimss_.size(); ++i)
    for (size_t j = 0; j < x.dimss_[i].size(); ++j)
      if (!boost::apply_visitor(*this, x.dimss_[i][j].expr_))
        return false;
  return true;
}
bool data_only_expression::operator()(const index_op_sliced &x) const {
  return boost::apply_visitor(*this, x.expr_.expr_);
}
bool data_only_expression::operator()(const conditional_op &x) const {
  return boost::apply_visitor(*this, x.cond_.expr_) &&
         boost::apply_visitor(*this, x.true_val_.expr_) &&
         boost::apply_visitor(*this, x.false_val_.expr_);
}
bool data_only_expression::operator()(const binary_op &x) const {
  return boost::apply_visitor(*this, x.left.expr_) &&
         boost::apply_visitor(*this, x.right.expr_);
}
bool data_only_expression::operator()(const unary_op &x) const {
  return boost::apply_visitor(*this, x.subject.expr_);
}

template <typename T>
void validate_definition::operator()(const scope &var_scope, const T &var_decl,
                                     bool &pass,
                                     std::stringstream &error_msgs) const {
  if (is_nil(var_decl.def()))
    return;

  // validate that assigment is allowed in this block
  if (!var_scope.allows_assignment()) {
    error_msgs << "Variable definition not possible in this block."
               << std::endl;
    pass = false;
  }

  // validate type
  bare_expr_type decl_type(var_decl.bare_type());
  bare_expr_type def_type = var_decl.def().bare_type();

  bool types_compatible =
      (decl_type == def_type) ||
      (decl_type.is_primitive() && def_type.is_primitive() &&
       decl_type.is_double_type() && def_type.is_int_type());

  if (!types_compatible) {
    error_msgs << "Variable definition base type mismatch,"
               << " variable declared as base type ";
    write_bare_expr_type(error_msgs, decl_type);
    error_msgs << " variable definition has base type ";
    write_bare_expr_type(error_msgs, def_type);
    pass = false;
  }
  // validate dims
  if (decl_type.num_dims() != def_type.num_dims()) {
    error_msgs << "Variable definition dimensions mismatch,"
               << " definition specifies " << decl_type.num_dims()
               << ", declaration specifies " << def_type.num_dims();
    pass = false;
  }
  return;
}
boost::phoenix::function<validate_definition> validate_definition_f;

template void validate_definition::
operator()(const scope &var_scope, const block_var_decl &var_decl, bool &pass,
           std::stringstream &error_msgs) const;

template void validate_definition::
operator()(const scope &var_scope, const local_var_decl &var_decl, bool &pass,
           std::stringstream &error_msgs) const;

template void validate_definition::
operator()(const scope &var_scope, const var_decl &var_decl, bool &pass,
           std::stringstream &error_msgs) const;

void validate_identifier::reserve(const std::string &w) {
  reserved_word_set_.insert(w);
}
bool validate_identifier::contains(const std::set<std::string> &s,
                                   const std::string &x) const {
  return s.find(x) != s.end();
}
bool validate_identifier::identifier_exists(
    const std::string &identifier) const {
  return contains(reserved_word_set_, identifier) ||
         (contains(function_signatures::instance().key_set(), identifier) &&
          !contains(const_fun_name_set_, identifier));
}

validate_identifier::validate_identifier() {
  // constant functions which may be used as identifiers
  const_fun_name_set_.insert("pi");
  const_fun_name_set_.insert("e");
  const_fun_name_set_.insert("sqrt2");
  const_fun_name_set_.insert("log2");
  const_fun_name_set_.insert("log10");
  const_fun_name_set_.insert("not_a_number");
  const_fun_name_set_.insert("positive_infinity");
  const_fun_name_set_.insert("negative_infinity");
  const_fun_name_set_.insert("epsilon");
  const_fun_name_set_.insert("negative_epsilon");
  const_fun_name_set_.insert("machine_precision");

  // illegal identifiers
  reserve("for");
  reserve("in");
  reserve("while");
  reserve("repeat");
  reserve("until");
  reserve("if");
  reserve("then");
  reserve("else");
  reserve("true");
  reserve("false");

  reserve("int");
  reserve("real");
  reserve("vector");
  reserve("unit_vector");
  reserve("simplex");
  reserve("ordered");
  reserve("positive_ordered");
  reserve("row_vector");
  reserve("matrix");
  reserve("cholesky_factor_cov");
  reserve("cholesky_factor_corr");
  reserve("cov_matrix");
  reserve("corr_matrix");

  reserve("target");

  reserve("model");
  reserve("data");
  reserve("parameters");
  reserve("quantities");
  reserve("transformed");
  reserve("generated");

  reserve("var");
  reserve("fvar");
  reserve("STAN_MAJOR");
  reserve("STAN_MINOR");
  reserve("STAN_PATCH");
  reserve("STAN_MATH_MAJOR");
  reserve("STAN_MATH_MINOR");
  reserve("STAN_MATH_PATCH");

  reserve("alignas");
  reserve("alignof");
  reserve("and");
  reserve("and_eq");
  reserve("asm");
  reserve("auto");
  reserve("bitand");
  reserve("bitor");
  reserve("bool");
  reserve("break");
  reserve("case");
  reserve("catch");
  reserve("char");
  reserve("char16_t");
  reserve("char32_t");
  reserve("class");
  reserve("compl");
  reserve("const");
  reserve("constexpr");
  reserve("const_cast");
  reserve("continue");
  reserve("decltype");
  reserve("default");
  reserve("delete");
  reserve("do");
  reserve("double");
  reserve("dynamic_cast");
  reserve("else");
  reserve("enum");
  reserve("explicit");
  reserve("export");
  reserve("extern");
  reserve("false");
  reserve("float");
  reserve("for");
  reserve("friend");
  reserve("goto");
  reserve("if");
  reserve("inline");
  reserve("int");
  reserve("long");
  reserve("mutable");
  reserve("namespace");
  reserve("new");
  reserve("noexcept");
  reserve("not");
  reserve("not_eq");
  reserve("nullptr");
  reserve("operator");
  reserve("or");
  reserve("or_eq");
  reserve("private");
  reserve("protected");
  reserve("public");
  reserve("register");
  reserve("reinterpret_cast");
  reserve("return");
  reserve("short");
  reserve("signed");
  reserve("sizeof");
  reserve("static");
  reserve("static_assert");
  reserve("static_cast");
  reserve("struct");
  reserve("switch");
  reserve("template");
  reserve("this");
  reserve("thread_local");
  reserve("throw");
  reserve("true");
  reserve("try");
  reserve("typedef");
  reserve("typeid");
  reserve("typename");
  reserve("union");
  reserve("unsigned");
  reserve("using");
  reserve("virtual");
  reserve("void");
  reserve("volatile");
  reserve("wchar_t");
  reserve("while");
  reserve("xor");
  reserve("xor_eq");

  // function names declared in signatures
  using stan::lang::function_signatures;
  using std::set;
  using std::string;
  const function_signatures &sigs = function_signatures::instance();

  set<string> fun_names = sigs.key_set();
  for (set<string>::iterator it = fun_names.begin(); it != fun_names.end();
       ++it)
    if (!contains(const_fun_name_set_, *it))
      reserve(*it);
}

// validates identifier shape
void validate_identifier::operator()(const std::string &identifier, bool &pass,
                                     std::stringstream &error_msgs) const {
  int len = identifier.size();
  if (len >= 2 && identifier[len - 1] == '_' && identifier[len - 2] == '_') {
    error_msgs << "Variable identifier (name) may"
               << " not end in double underscore (__)" << std::endl
               << "    found identifer=" << identifier << std::endl;
    pass = false;
    return;
  }
  size_t period_position = identifier.find('.');
  if (period_position != std::string::npos) {
    error_msgs << "Variable identifier may not contain a period (.)"
               << std::endl
               << "    found period at position (indexed from 0)="
               << period_position << std::endl
               << "    found identifier=" << identifier << std::endl;
    pass = false;
    return;
  }
  if (identifier_exists(identifier)) {
    error_msgs << "Variable identifier (name) may not be reserved word"
               << std::endl
               << "    found identifier=" << identifier << std::endl;
    pass = false;
    return;
  }
  pass = true;
}
boost::phoenix::function<validate_identifier> validate_identifier_f;

// copies single dimension from M to N if only M declared
void copy_square_cholesky_dimension_if_necessary::
operator()(cholesky_factor_cov_block_type &block_type) const {
  if (is_nil(block_type.N_))
    block_type.N_ = block_type.M_;
}
boost::phoenix::function<copy_square_cholesky_dimension_if_necessary>
    copy_square_cholesky_dimension_if_necessary_f;

void empty_range::operator()(range &r,
                             std::stringstream & /*error_msgs*/) const {
  r = range();
}
boost::phoenix::function<empty_range> empty_range_f;

void empty_offset_multiplier::
operator()(offset_multiplier &r, std::stringstream & /*error_msgs*/) const {
  r = offset_multiplier();
}
boost::phoenix::function<empty_offset_multiplier> empty_offset_multiplier_f;

void set_int_range_lower::operator()(range &range, const expression &expr,
                                     bool &pass,
                                     std::stringstream &error_msgs) const {
  range.low_ = expr;
  validate_int_expr validator;
  validator(expr, pass, error_msgs);
}
boost::phoenix::function<set_int_range_lower> set_int_range_lower_f;

void set_int_range_upper::operator()(range &range, const expression &expr,
                                     bool &pass,
                                     std::stringstream &error_msgs) const {
  range.high_ = expr;
  validate_int_expr validator;
  validator(expr, pass, error_msgs);
}
boost::phoenix::function<set_int_range_upper> set_int_range_upper_f;

void validate_int_data_only_expr::
operator()(const expression &expr, bool &pass, variable_map &var_map,
           std::stringstream &error_msgs) const {
  if (!expr.bare_type().is_int_type()) {
    error_msgs << "Dimension declaration requires expression"
               << " denoting integer; found type=" << expr.bare_type()
               << std::endl;
    pass = false;
    return;
  }
  data_only_expression vis(error_msgs, var_map);
  bool only_data_dimensions = boost::apply_visitor(vis, expr.expr_);
  pass = only_data_dimensions;
  return;
}
boost::phoenix::function<validate_int_data_only_expr>
    validate_int_data_only_expr_f;

void set_double_range_lower::operator()(range &range, const expression &expr,
                                        bool &pass,
                                        std::stringstream &error_msgs) const {
  range.low_ = expr;
  validate_double_expr validator;
  validator(expr, pass, error_msgs);
}
boost::phoenix::function<set_double_range_lower> set_double_range_lower_f;

void set_double_range_upper::operator()(range &range, const expression &expr,
                                        bool &pass,
                                        std::stringstream &error_msgs) const {
  range.high_ = expr;
  validate_double_expr validator;
  validator(expr, pass, error_msgs);
}
boost::phoenix::function<set_double_range_upper> set_double_range_upper_f;

void set_double_offset_multiplier_loc::
operator()(offset_multiplier &offset_multiplier, const expression &expr,
           bool &pass, std::stringstream &error_msgs) const {
  offset_multiplier.offset_ = expr;
  validate_double_expr validator;
  validator(expr, pass, error_msgs);
}
boost::phoenix::function<set_double_offset_multiplier_loc>
    set_double_offset_multiplier_offset_f;

void set_double_offset_multiplier_multiplier::
operator()(offset_multiplier &offset_multiplier, const expression &expr,
           bool &pass, std::stringstream &error_msgs) const {
  offset_multiplier.multiplier_ = expr;
  validate_double_expr validator;
  validator(expr, pass, error_msgs);
}
boost::phoenix::function<set_double_offset_multiplier_multiplier>
    set_double_offset_multiplier_multiplier_f;


void validate_array_block_var_decl::
operator()(block_var_decl &var_decl_result, const block_var_type &el_type,
           const std::string &name, const std::vector<expression> &dims,
           const expression &def, bool &pass, std::ostream &error_msgs) const {
  if (dims.size() == 0) {
    error_msgs << "Array type requires at least 1 dimension,"
               << " none found" << std::endl;
    pass = false;
    return;
  }
  if (el_type.bare_type().is_ill_formed_type()) {
    error_msgs << "Array variable declaration is ill formed,"
               << " variable name=" << name << std::endl;
    pass = false;
    return;
  }
  stan::lang::block_array_type bat(el_type, dims);
  block_var_decl result(name, bat, def);
  var_decl_result = result;
}
boost::phoenix::function<validate_array_block_var_decl>
    validate_array_block_var_decl_f;

void validate_single_block_var_decl::
operator()(const block_var_decl &var_decl, bool &pass,
           std::ostream &error_msgs) const {
  if (var_decl.bare_type().is_ill_formed_type()) {
    error_msgs << "Variable declaration is ill formed,"
               << " variable name=" << var_decl.name() << std::endl;
    pass = false;
    return;
  }
}
boost::phoenix::function<validate_single_block_var_decl>
    validate_single_block_var_decl_f;

void validate_single_local_var_decl::
operator()(const local_var_decl &var_decl, bool &pass,
           std::ostream &error_msgs) const {
  if (var_decl.bare_type().is_ill_formed_type()) {
    error_msgs << "Variable declaration is ill formed,"
               << " variable name=" << var_decl.name() << std::endl;
    pass = false;
    return;
  }
}
boost::phoenix::function<validate_single_local_var_decl>
    validate_single_local_var_decl_f;

void validate_array_local_var_decl::
operator()(local_var_decl &var_decl_result, const local_var_type &el_type,
           const std::string &name, const std::vector<expression> &dims,
           const expression &def, bool &pass, std::ostream &error_msgs) const {
  if (dims.size() == 0) {
    error_msgs << "Array type requires at least 1 dimension,"
               << " none found" << std::endl;
    pass = false;
    return;
  }
  if (el_type.bare_type().is_ill_formed_type()) {
    error_msgs << "Array variable declaration is ill formed,"
               << " variable name=" << name << std::endl;
    pass = false;
    return;
  }
  stan::lang::local_array_type bat(el_type, dims);
  local_var_decl result(name, bat, def);
  var_decl_result = result;
}
boost::phoenix::function<validate_array_local_var_decl>
    validate_array_local_var_decl_f;

void validate_fun_arg_var::operator()(var_decl &var_decl_result,
                                      const bare_expr_type &bare_type,
                                      const std::string &name, bool &pass,
                                      std::ostream &error_msgs) const {
  if (bare_type.is_ill_formed_type()) {
    error_msgs << "Function argument is ill formed,"
               << " name=" << name << std::endl;
    pass = false;
    return;
  }
  stan::lang::var_decl vd(name, bare_type);
  var_decl_result = vd;
}
boost::phoenix::function<validate_fun_arg_var> validate_fun_arg_var_f;

void validate_bare_type::operator()(bare_expr_type &bare_type_result,
                                    const bare_expr_type &el_type,
                                    const size_t &num_dims, bool &pass,
                                    std::ostream &error_msgs) const {
  if (el_type.is_ill_formed_type()) {
    error_msgs << "Ill-formed bare type" << std::endl;
    pass = false;
    return;
  }
  pass = true;
  if (num_dims == 0) {
    bare_type_result = el_type;
    return;
  }
  stan::lang::bare_array_type bat(el_type);
  for (size_t i = 0; i < num_dims - 1; ++i) {
    stan::lang::bare_expr_type cur_type(bat);
    bat = bare_array_type(cur_type);
  }
  bare_type_result = bat;
}
boost::phoenix::function<validate_bare_type> validate_bare_type_f;

template <typename T>
void add_to_var_map::operator()(const T &decl, variable_map &vm, bool &pass,
                                const scope &var_scope,
                                std::ostream &error_msgs) const {
  pass = false;
  if (vm.exists(decl.name())) {
    var_decl prev_decl = vm.get(decl.name());
    error_msgs << "Duplicate declaration of variable, name=" << decl.name();

    error_msgs << "; attempt to redeclare as " << decl.bare_type() << " in ";
    print_scope(error_msgs, var_scope);

    error_msgs << "; previously declared as " << prev_decl.bare_type()
               << " in ";
    print_scope(error_msgs, vm.get_scope(decl.name()));

    error_msgs << std::endl;
    pass = false;
    return;
  }
  if (var_scope.par_or_tpar() &&
      decl.bare_type().innermost_type().is_int_type()) {
    error_msgs << "Parameters or transformed parameters"
               << " cannot be integer or integer array; "
               << " found int variable declaration, name=" << decl.name()
               << std::endl;
    pass = false;
    return;
  }
  var_decl bare_decl(decl.name(), decl.type().bare_type(), decl.def());

  vm.add(decl.name(), bare_decl, var_scope);
  pass = true;
}
boost::phoenix::function<add_to_var_map> add_to_var_map_f;

template void add_to_var_map::operator()(const block_var_decl &decl,
                                         variable_map &vm, bool &pass,
                                         const scope &var_scope,
                                         std::ostream &error_msgs) const;

template void add_to_var_map::operator()(const local_var_decl &decl,
                                         variable_map &vm, bool &pass,
                                         const scope &var_scope,
                                         std::ostream &error_msgs) const;

void validate_in_loop::operator()(bool in_loop, bool &pass,
                                  std::ostream &error_msgs) const {
  pass = in_loop;
  if (!pass)
    error_msgs << "Break and continue statements are only allowed"
               << " in the body of a for-loop or while-loop." << std::endl;
}
boost::phoenix::function<validate_in_loop> validate_in_loop_f;

void non_void_expression::operator()(const expression &e, bool &pass,
                                     std::ostream &error_msgs) const {
  // ill-formed shouldn't be possible, but just in case
  pass = !(e.bare_type().is_void_type() || e.bare_type().is_ill_formed_type());
  if (!pass)
    error_msgs << "Error: expected printable (non-void) expression."
               << std::endl;
}
boost::phoenix::function<non_void_expression> non_void_expression_f;

void set_var_scope::operator()(scope &var_scope,
                               const origin_block &program_block) const {
  var_scope = scope(program_block);
}
boost::phoenix::function<set_var_scope> set_var_scope_f;

void set_data_origin::operator()(scope &var_scope) const {
  var_scope = scope(data_origin);
}
boost::phoenix::function<set_data_origin> set_data_origin_f;

void set_var_scope_local::operator()(scope &var_scope,
                                     const origin_block &program_block) const {
  var_scope = scope(program_block, true);
}
boost::phoenix::function<set_var_scope_local> set_var_scope_local_f;

void reset_var_scope::operator()(scope &var_scope,
                                 const scope &scope_enclosing) const {
  origin_block enclosing_block = scope_enclosing.program_block();
  var_scope = scope(enclosing_block, true);
}
boost::phoenix::function<reset_var_scope> reset_var_scope_f;

// only used to debug grammars
void trace::operator()(const std::string &msg) const {
  //      std::cout << msg << std::endl;
}
boost::phoenix::function<trace> trace_f;

// only used to debug grammars
void trace_pass::operator()(const std::string &msg, const bool &pass) const {
  //      std::cout << msg << " pass? " << pass << std::endl;
}
boost::phoenix::function<trace_pass> trace_pass_f;

void deprecate_pound_comment::operator()(std::ostream &error_msgs) const {
  error_msgs << "Info: Comments beginning with #"
             << " are deprecated.  Please use // in place of #"
             << " for line comments." << std::endl;
}
boost::phoenix::function<deprecate_pound_comment> deprecate_pound_comment_f;

}  // namespace lang
}  // namespace stan
#endif
