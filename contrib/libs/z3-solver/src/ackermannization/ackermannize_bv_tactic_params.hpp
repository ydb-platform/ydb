// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct ackermannize_bv_tactic_params {
  params_ref const & p;
  params_ref g;
  ackermannize_bv_tactic_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("div0_ackermann_limit", CPK_UINT, "a bound for number of congruence Ackermann lemmas for div0 modelling", "1000","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'ackermannize_bv_tactic_params::collect_param_descrs')
  */
  unsigned div0_ackermann_limit() const { return p.get_uint("div0_ackermann_limit", g, 1000u); }
};
