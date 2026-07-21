// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct parser_params {
  params_ref const & p;
  params_ref g;
  parser_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("parser")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("ignore_user_patterns", CPK_BOOL, "ignore patterns provided by the user", "false","parser");
    d.insert("ignore_bad_patterns", CPK_BOOL, "ignore malformed patterns", "true","parser");
    d.insert("error_for_visual_studio", CPK_BOOL, "display error messages in Visual Studio format", "false","parser");
  }
  /*
     REG_MODULE_PARAMS('parser', 'parser_params::collect_param_descrs')
  */
  bool ignore_user_patterns() const { return p.get_bool("ignore_user_patterns", g, false); }
  bool ignore_bad_patterns() const { return p.get_bool("ignore_bad_patterns", g, true); }
  bool error_for_visual_studio() const { return p.get_bool("error_for_visual_studio", g, false); }
};
