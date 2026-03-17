// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct combined_solver_params {
  params_ref const & p;
  params_ref g;
  combined_solver_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("combined_solver")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("solver2_timeout", CPK_UINT, "fallback to solver 1 after timeout even when in incremental model", "4294967295","combined_solver");
    d.insert("ignore_solver1", CPK_BOOL, "if true, solver 2 is always used", "false","combined_solver");
    d.insert("solver2_unknown", CPK_UINT, "what should be done when solver 2 returns unknown: 0 - just return unknown, 1 - execute solver 1 if quantifier free problem, 2 - execute solver 1", "1","combined_solver");
  }
  /*
     REG_MODULE_PARAMS('combined_solver', 'combined_solver_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('combined_solver', 'combines two solvers: non-incremental (solver1) and incremental (solver2)')
  */
  unsigned solver2_timeout() const { return p.get_uint("solver2_timeout", g, 4294967295u); }
  bool ignore_solver1() const { return p.get_bool("ignore_solver1", g, false); }
  unsigned solver2_unknown() const { return p.get_uint("solver2_unknown", g, 1u); }
};
