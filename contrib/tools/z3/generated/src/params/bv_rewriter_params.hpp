// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct bv_rewriter_params {
  params_ref const & p;
  params_ref g;
  bv_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("split_concat_eq", CPK_BOOL, "split equalities of the form (= (concat t1 t2) t3)", "false","rewriter");
    d.insert("bit2bool", CPK_BOOL, "try to convert bit-vector terms of size 1 into Boolean terms", "true","rewriter");
    d.insert("blast_eq_value", CPK_BOOL, "blast (some) Bit-vector equalities into bits", "false","rewriter");
    d.insert("elim_sign_ext", CPK_BOOL, "expand sign-ext operator using concat and extract", "true","rewriter");
    d.insert("hi_div0", CPK_BOOL, "use the 'hardware interpretation' for division by zero (for bit-vector terms)", "true","rewriter");
    d.insert("mul2concat", CPK_BOOL, "replace multiplication by a power of two into a concatenation", "false","rewriter");
    d.insert("bv_sort_ac", CPK_BOOL, "sort the arguments of all AC operators", "false","rewriter");
    d.insert("bv_extract_prop", CPK_BOOL, "attempt to partially propagate extraction inwards", "false","rewriter");
    d.insert("bv_not_simpl", CPK_BOOL, "apply simplifications for bvnot", "false","rewriter");
    d.insert("bv_ite2id", CPK_BOOL, "rewrite ite that can be simplified to identity", "false","rewriter");
    d.insert("bv_le_extra", CPK_BOOL, "additional bu_(u/s)le simplifications", "false","rewriter");
    d.insert("bv_le2extract", CPK_BOOL, "disassemble bvule to extract", "true","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'bv_rewriter_params::collect_param_descrs')
  */
  bool split_concat_eq() const { return p.get_bool("split_concat_eq", g, false); }
  bool bit2bool() const { return p.get_bool("bit2bool", g, true); }
  bool blast_eq_value() const { return p.get_bool("blast_eq_value", g, false); }
  bool elim_sign_ext() const { return p.get_bool("elim_sign_ext", g, true); }
  bool hi_div0() const { return p.get_bool("hi_div0", g, true); }
  bool mul2concat() const { return p.get_bool("mul2concat", g, false); }
  bool bv_sort_ac() const { return p.get_bool("bv_sort_ac", g, false); }
  bool bv_extract_prop() const { return p.get_bool("bv_extract_prop", g, false); }
  bool bv_not_simpl() const { return p.get_bool("bv_not_simpl", g, false); }
  bool bv_ite2id() const { return p.get_bool("bv_ite2id", g, false); }
  bool bv_le_extra() const { return p.get_bool("bv_le_extra", g, false); }
  bool bv_le2extract() const { return p.get_bool("bv_le2extract", g, true); }
};
