// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct rcf_params {
  params_ref const & p;
  params_ref g;
  rcf_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rcf")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("use_prem", CPK_BOOL, "use pseudo-remainder instead of remainder when computing GCDs and Sturm-Tarski sequences", "true","rcf");
    d.insert("clean_denominators", CPK_BOOL, "clean denominators before root isolation", "true","rcf");
    d.insert("initial_precision", CPK_UINT, "a value k that is the initial interval size (as 1/2^k) when creating transcendentals and approximated division", "24","rcf");
    d.insert("inf_precision", CPK_UINT, "a value k that is the initial interval size (i.e., (0, 1/2^l)) used as an approximation for infinitesimal values", "24","rcf");
    d.insert("max_precision", CPK_UINT, "during sign determination we switch from interval arithmetic to complete methods when the interval size is less than 1/2^k, where k is the max_precision", "128","rcf");
    d.insert("lazy_algebraic_normalization", CPK_BOOL, "during sturm-seq and square-free polynomial computations, only normalize algebraic polynomial expressions when the defining polynomial is monic", "true","rcf");
  }
  /*
     REG_MODULE_PARAMS('rcf', 'rcf_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('rcf', 'real closed fields')
  */
  bool use_prem() const { return p.get_bool("use_prem", g, true); }
  bool clean_denominators() const { return p.get_bool("clean_denominators", g, true); }
  unsigned initial_precision() const { return p.get_uint("initial_precision", g, 24u); }
  unsigned inf_precision() const { return p.get_uint("inf_precision", g, 24u); }
  unsigned max_precision() const { return p.get_uint("max_precision", g, 128u); }
  bool lazy_algebraic_normalization() const { return p.get_bool("lazy_algebraic_normalization", g, true); }
};
