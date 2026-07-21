// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct poly_rewriter_params {
  params_ref const & p;
  params_ref g;
  poly_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("som", CPK_BOOL, "put polynomials in sum-of-monomials form", "false","rewriter");
    d.insert("som_blowup", CPK_UINT, "maximum increase of monomials generated when putting a polynomial in sum-of-monomials normal form", "10","rewriter");
    d.insert("hoist_mul", CPK_BOOL, "hoist multiplication over summation to minimize number of multiplications", "false","rewriter");
    d.insert("hoist_ite", CPK_BOOL, "hoist shared summands under ite expressions", "false","rewriter");
    d.insert("flat", CPK_BOOL, "create nary applications for and,or,+,*,bvadd,bvmul,bvand,bvor,bvxor", "true","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'poly_rewriter_params::collect_param_descrs')
  */
  bool som() const { return p.get_bool("som", g, false); }
  unsigned som_blowup() const { return p.get_uint("som_blowup", g, 10u); }
  bool hoist_mul() const { return p.get_bool("hoist_mul", g, false); }
  bool hoist_ite() const { return p.get_bool("hoist_ite", g, false); }
  bool flat() const { return p.get_bool("flat", g, true); }
};
