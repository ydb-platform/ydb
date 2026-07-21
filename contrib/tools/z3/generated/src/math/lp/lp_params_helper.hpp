// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct lp_params_helper {
  params_ref const & p;
  params_ref g;
  lp_params_helper(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("lp")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("dio", CPK_BOOL, "use Diophantine equalities", "true","lp");
    d.insert("dio_branching_period", CPK_UINT, "Period of calling branching on undef in Diophantine handler", "100","lp");
    d.insert("dio_cuts_enable_gomory", CPK_BOOL, "enable Gomory cuts together with Diophantine cuts, only relevant when dioph_eq is true", "false","lp");
    d.insert("dio_cuts_enable_hnf", CPK_BOOL, "enable hnf cuts together with Diophantine cuts, only relevant when dioph_eq is true", "true","lp");
    d.insert("dio_ignore_big_nums", CPK_BOOL, "Ignore the terms with big numbers in the Diophantine handler, only relevant when dioph_eq is true", "true","lp");
    d.insert("dio_calls_period", CPK_UINT, "Period of calling the Diophantine handler in the final_check()", "1","lp");
    d.insert("dio_run_gcd", CPK_BOOL, "Run the GCD heuristic if dio is on, if dio is disabled the option is not used", "false","lp");
  }
  /*
     REG_MODULE_PARAMS('lp', 'lp_params_helper::collect_param_descrs')
     REG_MODULE_DESCRIPTION('lp', 'linear programming parameters')
  */
  bool dio() const { return p.get_bool("dio", g, true); }
  unsigned dio_branching_period() const { return p.get_uint("dio_branching_period", g, 100u); }
  bool dio_cuts_enable_gomory() const { return p.get_bool("dio_cuts_enable_gomory", g, false); }
  bool dio_cuts_enable_hnf() const { return p.get_bool("dio_cuts_enable_hnf", g, true); }
  bool dio_ignore_big_nums() const { return p.get_bool("dio_ignore_big_nums", g, true); }
  unsigned dio_calls_period() const { return p.get_uint("dio_calls_period", g, 1u); }
  bool dio_run_gcd() const { return p.get_bool("dio_run_gcd", g, false); }
};
