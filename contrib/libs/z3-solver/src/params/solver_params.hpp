// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct solver_params {
  params_ref const & p;
  params_ref g;
  solver_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("solver")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("smtlib2_log", CPK_SYMBOL, "file to save solver interaction", "","solver");
    d.insert("cancel_backup_file", CPK_SYMBOL, "file to save partial search state if search is canceled", "","solver");
    d.insert("timeout", CPK_UINT, "timeout on the solver object; overwrites a global timeout", "4294967295","solver");
    d.insert("lemmas2console", CPK_BOOL, "print lemmas during search", "false","solver");
    d.insert("instantiations2console", CPK_BOOL, "print quantifier instantiations to the console", "false","solver");
    d.insert("axioms2files", CPK_BOOL, "print negated theory axioms to separate files during search", "false","solver");
    d.insert("slice", CPK_BOOL, "use slice solver that filters assertions to use symbols occuring in @query formulas", "false","solver");
    d.insert("proof.log", CPK_SYMBOL, "log clause proof trail into a file", "","solver");
    d.insert("proof.check", CPK_BOOL, "check proof logs", "true","solver");
    d.insert("proof.check_rup", CPK_BOOL, "check proof RUP inference in proof logs", "true","solver");
    d.insert("proof.save", CPK_BOOL, "save proof log into a proof object that can be extracted using (get-proof)", "false","solver");
    d.insert("proof.trim", CPK_BOOL, "trim and save proof into a proof object that an be extracted using (get-proof)", "false","solver");
  }
  /*
     REG_MODULE_PARAMS('solver', 'solver_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('solver', 'solver parameters')
  */
  symbol smtlib2_log() const { return p.get_sym("smtlib2_log", g, symbol("")); }
  symbol cancel_backup_file() const { return p.get_sym("cancel_backup_file", g, symbol("")); }
  unsigned timeout() const { return p.get_uint("timeout", g, 4294967295u); }
  bool lemmas2console() const { return p.get_bool("lemmas2console", g, false); }
  bool instantiations2console() const { return p.get_bool("instantiations2console", g, false); }
  bool axioms2files() const { return p.get_bool("axioms2files", g, false); }
  bool slice() const { return p.get_bool("slice", g, false); }
  symbol proof_log() const { return p.get_sym("proof.log", g, symbol("")); }
  bool proof_check() const { return p.get_bool("proof.check", g, true); }
  bool proof_check_rup() const { return p.get_bool("proof.check_rup", g, true); }
  bool proof_save() const { return p.get_bool("proof.save", g, false); }
  bool proof_trim() const { return p.get_bool("proof.trim", g, false); }
};
