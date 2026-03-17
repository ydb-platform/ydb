// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct sat_scc_params {
  params_ref const & p;
  params_ref g;
  sat_scc_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("sat")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("scc", CPK_BOOL, "eliminate Boolean variables by computing strongly connected components", "true","sat");
    d.insert("scc.tr", CPK_BOOL, "apply transitive reduction, eliminate redundant binary clauses", "true","sat");
  }
  /*
     REG_MODULE_PARAMS('sat', 'sat_scc_params::collect_param_descrs')
  */
  bool scc() const { return p.get_bool("scc", g, true); }
  bool scc_tr() const { return p.get_bool("scc.tr", g, true); }
};
