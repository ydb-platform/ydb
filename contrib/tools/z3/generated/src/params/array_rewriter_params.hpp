// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct array_rewriter_params {
  params_ref const & p;
  params_ref g;
  array_rewriter_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("rewriter")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("expand_select_store", CPK_BOOL, "conservatively replace a (select (store ...) ...) term by an if-then-else term", "false","rewriter");
    d.insert("blast_select_store", CPK_BOOL, "eagerly replace all (select (store ..) ..) term by an if-then-else term", "false","rewriter");
    d.insert("expand_nested_stores", CPK_BOOL, "replace nested stores by a lambda expression", "false","rewriter");
    d.insert("expand_select_ite", CPK_BOOL, "expand select over ite expressions", "false","rewriter");
    d.insert("expand_store_eq", CPK_BOOL, "reduce (store ...) = (store ...) with a common base into selects", "false","rewriter");
    d.insert("sort_store", CPK_BOOL, "sort nested stores when the indices are known to be different", "false","rewriter");
  }
  /*
     REG_MODULE_PARAMS('rewriter', 'array_rewriter_params::collect_param_descrs')
  */
  bool expand_select_store() const { return p.get_bool("expand_select_store", g, false); }
  bool blast_select_store() const { return p.get_bool("blast_select_store", g, false); }
  bool expand_nested_stores() const { return p.get_bool("expand_nested_stores", g, false); }
  bool expand_select_ite() const { return p.get_bool("expand_select_ite", g, false); }
  bool expand_store_eq() const { return p.get_bool("expand_store_eq", g, false); }
  bool sort_store() const { return p.get_bool("sort_store", g, false); }
};
