// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct nnf_params {
  params_ref const & p;
  params_ref g;
  nnf_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("nnf")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("max_memory", CPK_UINT, "maximum amount of memory in megabytes", "4294967295","nnf");
    d.insert("sk_hack", CPK_BOOL, "hack for VCC", "false","nnf");
    d.insert("mode", CPK_SYMBOL, "NNF translation mode: skolem (skolem normal form), quantifiers (skolem normal form + quantifiers in NNF), full", "skolem","nnf");
    d.insert("ignore_labels", CPK_BOOL, "remove/ignore labels in the input formula, this option is ignored if proofs are enabled", "false","nnf");
  }
  /*
     REG_MODULE_PARAMS('nnf', 'nnf_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('nnf', 'negation normal form')
  */
  unsigned max_memory() const { return p.get_uint("max_memory", g, 4294967295u); }
  bool sk_hack() const { return p.get_bool("sk_hack", g, false); }
  symbol mode() const { return p.get_sym("mode", g, symbol("skolem")); }
  bool ignore_labels() const { return p.get_bool("ignore_labels", g, false); }
};
