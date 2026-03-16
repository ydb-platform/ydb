// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct fpa2bv_rewriter_params {
  params_ref const & p;
  params_ref g;
  fpa2bv_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("hi_fp_unspecified", CPK_BOOL, "use the 'hardware interpretation' for unspecified values in fp.min, fp.max, fp.to_ubv, fp.to_sbv, and fp.to_real", "false","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'fpa2bv_rewriter_params::collect_param_descrs')
  */
  bool hi_fp_unspecified() const { return p.get_bool("hi_fp_unspecified", g, false); }
};
