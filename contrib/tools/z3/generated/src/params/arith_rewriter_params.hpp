// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct arith_rewriter_params {
  params_ref const & p;
  params_ref g;
  arith_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("algebraic_number_evaluator", CPK_BOOL, "simplify/evaluate expressions containing (algebraic) irrational numbers.", "true","rewriter");
    d.insert("mul_to_power", CPK_BOOL, "collpase (* t ... t) into (^ t k), it is ignored if expand_power is true.", "false","rewriter");
    d.insert("expand_power", CPK_BOOL, "expand (^ t k) into (* t ... t) if  1 < k <= max_degree.", "false","rewriter");
    d.insert("expand_tan", CPK_BOOL, "replace (tan x) with (/ (sin x) (cos x)).", "false","rewriter");
    d.insert("max_degree", CPK_UINT, "max degree of algebraic numbers (and power operators) processed by simplifier.", "64","rewriter");
    d.insert("sort_sums", CPK_BOOL, "sort the arguments of + application.", "false","rewriter");
    d.insert("gcd_rounding", CPK_BOOL, "use gcd rounding on integer arithmetic atoms.", "false","rewriter");
    d.insert("arith_lhs", CPK_BOOL, "all monomials are moved to the left-hand-side, and the right-hand-side is just a constant.", "false","rewriter");
    d.insert("arith_ineq_lhs", CPK_BOOL, "rewrite inequalities so that right-hand-side is a constant.", "false","rewriter");
    d.insert("elim_to_real", CPK_BOOL, "eliminate to_real from arithmetic predicates that contain only integers.", "false","rewriter");
    d.insert("push_to_real", CPK_BOOL, "distribute to_real over * and +.", "true","rewriter");
    d.insert("eq2ineq", CPK_BOOL, "expand equalities into two inequalities", "false","rewriter");
    d.insert("elim_rem", CPK_BOOL, "replace (rem x y) with (ite (>= y 0) (mod x y) (- (mod x y))).", "false","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'arith_rewriter_params::collect_param_descrs')
  */
  bool algebraic_number_evaluator() const { return p.get_bool("algebraic_number_evaluator", g, true); }
  bool mul_to_power() const { return p.get_bool("mul_to_power", g, false); }
  bool expand_power() const { return p.get_bool("expand_power", g, false); }
  bool expand_tan() const { return p.get_bool("expand_tan", g, false); }
  unsigned max_degree() const { return p.get_uint("max_degree", g, 64u); }
  bool sort_sums() const { return p.get_bool("sort_sums", g, false); }
  bool gcd_rounding() const { return p.get_bool("gcd_rounding", g, false); }
  bool arith_lhs() const { return p.get_bool("arith_lhs", g, false); }
  bool arith_ineq_lhs() const { return p.get_bool("arith_ineq_lhs", g, false); }
  bool elim_to_real() const { return p.get_bool("elim_to_real", g, false); }
  bool push_to_real() const { return p.get_bool("push_to_real", g, true); }
  bool eq2ineq() const { return p.get_bool("eq2ineq", g, false); }
  bool elim_rem() const { return p.get_bool("elim_rem", g, false); }
};
