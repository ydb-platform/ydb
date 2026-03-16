// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct seq_rewriter_params {
  params_ref const & p;
  params_ref g;
  seq_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("coalesce_chars", CPK_BOOL, "coalesce characters into strings", "true","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'seq_rewriter_params::collect_param_descrs')
  */
  bool coalesce_chars() const { return p.get_bool("coalesce_chars", g, true); }
};
