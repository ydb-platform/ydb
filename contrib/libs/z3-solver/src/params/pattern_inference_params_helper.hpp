// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct pattern_inference_params_helper {
  params_ref const & p;
  params_ref g;
  pattern_inference_params_helper(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("pi")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("max_multi_patterns", CPK_UINT, "when patterns are not provided, the prover uses a heuristic to infer them, this option sets the threshold on the number of extra multi-patterns that can be created; by default, the prover creates at most one multi-pattern when there is no unary pattern", "0","pi");
    d.insert("block_loop_patterns", CPK_BOOL, "block looping patterns during pattern inference", "true","pi");
    d.insert("decompose_patterns", CPK_BOOL, "allow decomposition of patterns into multipatterns", "true","pi");
    d.insert("arith", CPK_UINT, "0 - do not infer patterns with arithmetic terms, 1 - use patterns with arithmetic terms if there is no other pattern, 2 - always use patterns with arithmetic terms", "1","pi");
    d.insert("use_database", CPK_BOOL, "use pattern database", "false","pi");
    d.insert("enabled", CPK_BOOL, "enable a heuristic to infer patterns, when they are not provided", "true","pi");
    d.insert("arith_weight", CPK_UINT, "default weight for quantifiers where the only available pattern has nested arithmetic terms", "5","pi");
    d.insert("non_nested_arith_weight", CPK_UINT, "default weight for quantifiers where the only available pattern has non nested arithmetic terms", "10","pi");
    d.insert("pull_quantifiers", CPK_BOOL, "pull nested quantifiers, if no pattern was found", "true","pi");
    d.insert("warnings", CPK_BOOL, "enable/disable warning messages in the pattern inference module", "false","pi");
  }
  /*
     REG_MODULE_PARAMS('pi', 'pattern_inference_params_helper::collect_param_descrs')
     REG_MODULE_DESCRIPTION('pi', 'pattern inference (heuristics) for universal formulas (without annotation)')
  */
  unsigned max_multi_patterns() const { return p.get_uint("max_multi_patterns", g, 0u); }
  bool block_loop_patterns() const { return p.get_bool("block_loop_patterns", g, true); }
  bool decompose_patterns() const { return p.get_bool("decompose_patterns", g, true); }
  unsigned arith() const { return p.get_uint("arith", g, 1u); }
  bool use_database() const { return p.get_bool("use_database", g, false); }
  bool enabled() const { return p.get_bool("enabled", g, true); }
  unsigned arith_weight() const { return p.get_uint("arith_weight", g, 5u); }
  unsigned non_nested_arith_weight() const { return p.get_uint("non_nested_arith_weight", g, 10u); }
  bool pull_quantifiers() const { return p.get_bool("pull_quantifiers", g, true); }
  bool warnings() const { return p.get_bool("warnings", g, false); }
};
