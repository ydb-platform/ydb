// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct smt_parallel_params {
  params_ref const & p;
  params_ref g;
  smt_parallel_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("smt_parallel")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("inprocessing", CPK_BOOL, "integrate in-processing as a heuristic simplification", "false","smt_parallel");
    d.insert("sls", CPK_BOOL, "add sls-tactic as a separate worker thread outside the search tree parallelism", "false","smt_parallel");
  }
  /*
     REG_MODULE_PARAMS('smt_parallel', 'smt_parallel_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('smt_parallel', 'Experimental parameters for parallel solving')
  */
  bool inprocessing() const { return p.get_bool("inprocessing", g, false); }
  bool sls() const { return p.get_bool("sls", g, false); }
};
