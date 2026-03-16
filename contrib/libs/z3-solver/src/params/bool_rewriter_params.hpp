// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct bool_rewriter_params {
  params_ref const & p;
  params_ref g;
  bool_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("ite_extra_rules", CPK_BOOL, "extra ite simplifications, these additional simplifications may reduce size locally but increase globally", "true","rewriter");
    d.insert("flat", CPK_BOOL, "create nary applications for +,*,bvadd,bvmul,bvand,bvor,bvxor", "true","rewriter");
    d.insert("flat_and_or", CPK_BOOL, "create nary applications for and,or", "true","rewriter");
    d.insert("sort_disjunctions", CPK_BOOL, "sort subterms in disjunctions", "true","rewriter");
    d.insert("elim_and", CPK_BOOL, "conjunctions are rewritten using negation and disjunctions", "false","rewriter");
    d.insert("elim_ite", CPK_BOOL, "eliminate ite in favor of and/or", "true","rewriter");
    d.insert("local_ctx", CPK_BOOL, "perform local (i.e., cheap) context simplifications", "false","rewriter");
    d.insert("local_ctx_limit", CPK_UINT, "limit for applying local context simplifier", "4294967295","rewriter");
    d.insert("blast_distinct", CPK_BOOL, "expand a distinct predicate into a quadratic number of disequalities", "false","rewriter");
    d.insert("blast_distinct_threshold", CPK_UINT, "when blast_distinct is true, only distinct expressions with less than this number of arguments are blasted", "4294967295","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'bool_rewriter_params::collect_param_descrs')
  */
  bool ite_extra_rules() const { return p.get_bool("ite_extra_rules", g, true); }
  bool flat() const { return p.get_bool("flat", g, true); }
  bool flat_and_or() const { return p.get_bool("flat_and_or", g, true); }
  bool sort_disjunctions() const { return p.get_bool("sort_disjunctions", g, true); }
  bool elim_and() const { return p.get_bool("elim_and", g, false); }
  bool elim_ite() const { return p.get_bool("elim_ite", g, true); }
  bool local_ctx() const { return p.get_bool("local_ctx", g, false); }
  unsigned local_ctx_limit() const { return p.get_uint("local_ctx_limit", g, 4294967295u); }
  bool blast_distinct() const { return p.get_bool("blast_distinct", g, false); }
  unsigned blast_distinct_threshold() const { return p.get_uint("blast_distinct_threshold", g, 4294967295u); }
};
