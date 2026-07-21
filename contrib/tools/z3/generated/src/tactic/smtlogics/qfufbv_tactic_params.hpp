// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct qfufbv_tactic_params {
  params_ref const & p;
  params_ref g;
  qfufbv_tactic_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("ackermannization")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("sat_backend", CPK_BOOL, "use SAT rather than SMT in qfufbv_ackr_tactic", "false","ackermannization");
    d.insert("inc_sat_backend", CPK_BOOL, "use incremental SAT", "false","ackermannization");
  }
  /*
     REG_MODULE_PARAMS('ackermannization', 'qfufbv_tactic_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('ackermannization', 'tactics based on solving UF-theories via ackermannization (see also ackr module)')
  */
  bool sat_backend() const { return p.get_bool("sat_backend", g, false); }
  bool inc_sat_backend() const { return p.get_bool("inc_sat_backend", g, false); }
};
