// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct model_params {
  params_ref const & p;
  params_ref g;
  model_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("model")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("partial", CPK_BOOL, "enable/disable partial function interpretations", "false","model");
    d.insert("v1", CPK_BOOL, "use Z3 version 1.x pretty printer", "false","model");
    d.insert("v2", CPK_BOOL, "use Z3 version 2.x (x <= 16) pretty printer", "false","model");
    d.insert("compact", CPK_BOOL, "try to compact function graph (i.e., function interpretations that are lookup tables)", "true","model");
    d.insert("inline_def", CPK_BOOL, "inline local function definitions ignoring possible expansion", "false","model");
    d.insert("user_functions", CPK_BOOL, "include user defined functions in model", "true","model");
    d.insert("completion", CPK_BOOL, "enable/disable model completion", "false","model");
  }
  /*
     REG_MODULE_PARAMS('model', 'model_params::collect_param_descrs')
  */
  bool partial() const { return p.get_bool("partial", g, false); }
  bool v1() const { return p.get_bool("v1", g, false); }
  bool v2() const { return p.get_bool("v2", g, false); }
  bool compact() const { return p.get_bool("compact", g, true); }
  bool inline_def() const { return p.get_bool("inline_def", g, false); }
  bool user_functions() const { return p.get_bool("user_functions", g, true); }
  bool completion() const { return p.get_bool("completion", g, false); }
};
