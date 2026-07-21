// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct sat_asymm_branch_params {
  params_ref const & p;
  params_ref g;
  sat_asymm_branch_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("sat")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("asymm_branch", CPK_BOOL, "asymmetric branching", "true","sat");
    d.insert("asymm_branch.rounds", CPK_UINT, "maximal number of rounds to run asymmetric branch simplifications if progress is made", "2","sat");
    d.insert("asymm_branch.delay", CPK_UINT, "number of simplification rounds to wait until invoking asymmetric branch simplification", "1","sat");
    d.insert("asymm_branch.sampled", CPK_BOOL, "use sampling based asymmetric branching based on binary implication graph", "true","sat");
    d.insert("asymm_branch.limit", CPK_UINT, "approx. maximum number of literals visited during asymmetric branching", "100000000","sat");
    d.insert("asymm_branch.all", CPK_BOOL, "asymmetric branching on all literals per clause", "false","sat");
  }
  /*
     REG_MODULE_PARAMS('sat', 'sat_asymm_branch_params::collect_param_descrs')
  */
  bool asymm_branch() const { return p.get_bool("asymm_branch", g, true); }
  unsigned asymm_branch_rounds() const { return p.get_uint("asymm_branch.rounds", g, 2u); }
  unsigned asymm_branch_delay() const { return p.get_uint("asymm_branch.delay", g, 1u); }
  bool asymm_branch_sampled() const { return p.get_bool("asymm_branch.sampled", g, true); }
  unsigned asymm_branch_limit() const { return p.get_uint("asymm_branch.limit", g, 100000000u); }
  bool asymm_branch_all() const { return p.get_bool("asymm_branch.all", g, false); }
};
