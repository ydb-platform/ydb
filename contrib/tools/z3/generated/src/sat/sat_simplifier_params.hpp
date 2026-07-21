// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct sat_simplifier_params {
  params_ref const & p;
  params_ref g;
  sat_simplifier_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("sat")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("bce", CPK_BOOL, "eliminate blocked clauses", "false","sat");
    d.insert("abce", CPK_BOOL, "eliminate blocked clauses using asymmetric literals", "false","sat");
    d.insert("cce", CPK_BOOL, "eliminate covered clauses", "false","sat");
    d.insert("ate", CPK_BOOL, "asymmetric tautology elimination", "true","sat");
    d.insert("acce", CPK_BOOL, "eliminate covered clauses using asymmetric added literals", "false","sat");
    d.insert("bce_at", CPK_UINT, "eliminate blocked clauses only once at the given simplification round", "2","sat");
    d.insert("bca", CPK_BOOL, "blocked clause addition - add blocked binary clauses", "false","sat");
    d.insert("bce_delay", CPK_UINT, "delay eliminate blocked clauses until simplification round", "2","sat");
    d.insert("retain_blocked_clauses", CPK_BOOL, "retain blocked clauses as lemmas", "true","sat");
    d.insert("blocked_clause_limit", CPK_UINT, "maximum number of literals visited during blocked clause elimination", "100000000","sat");
    d.insert("override_incremental", CPK_BOOL, "override incremental safety gaps. Enable elimination of blocked clauses and variables even if solver is reused", "false","sat");
    d.insert("resolution.limit", CPK_UINT, "approx. maximum number of literals visited during variable elimination", "500000000","sat");
    d.insert("resolution.occ_cutoff", CPK_UINT, "first cutoff (on number of positive/negative occurrences) for Boolean variable elimination", "10","sat");
    d.insert("resolution.occ_cutoff_range1", CPK_UINT, "second cutoff (number of positive/negative occurrences) for Boolean variable elimination, for problems containing less than res_cls_cutoff1 clauses", "8","sat");
    d.insert("resolution.occ_cutoff_range2", CPK_UINT, "second cutoff (number of positive/negative occurrences) for Boolean variable elimination, for problems containing more than res_cls_cutoff1 and less than res_cls_cutoff2", "5","sat");
    d.insert("resolution.occ_cutoff_range3", CPK_UINT, "second cutoff (number of positive/negative occurrences) for Boolean variable elimination, for problems containing more than res_cls_cutoff2", "3","sat");
    d.insert("resolution.lit_cutoff_range1", CPK_UINT, "second cutoff (total number of literals) for Boolean variable elimination, for problems containing less than res_cls_cutoff1 clauses", "700","sat");
    d.insert("resolution.lit_cutoff_range2", CPK_UINT, "second cutoff (total number of literals) for Boolean variable elimination, for problems containing more than res_cls_cutoff1 and less than res_cls_cutoff2", "400","sat");
    d.insert("resolution.lit_cutoff_range3", CPK_UINT, "second cutoff (total number of literals) for Boolean variable elimination, for problems containing more than res_cls_cutoff2", "300","sat");
    d.insert("resolution.cls_cutoff1", CPK_UINT, "limit1 - total number of problems clauses for the second cutoff of Boolean variable elimination", "100000000","sat");
    d.insert("resolution.cls_cutoff2", CPK_UINT, "limit2 - total number of problems clauses for the second cutoff of Boolean variable elimination", "700000000","sat");
    d.insert("elim_vars", CPK_BOOL, "enable variable elimination using resolution during simplification", "true","sat");
    d.insert("probing", CPK_BOOL, "apply failed literal detection during simplification", "true","sat");
    d.insert("probing_limit", CPK_UINT, "limit to the number of probe calls", "5000000","sat");
    d.insert("probing_cache", CPK_BOOL, "add binary literals as lemmas", "true","sat");
    d.insert("probing_cache_limit", CPK_UINT, "cache binaries unless overall memory usage exceeds cache limit", "1024","sat");
    d.insert("probing_binary", CPK_BOOL, "probe binary clauses", "true","sat");
    d.insert("subsumption", CPK_BOOL, "eliminate subsumed clauses", "true","sat");
    d.insert("subsumption.limit", CPK_UINT, "approx. maximum number of literals visited during subsumption (and subsumption resolution)", "100000000","sat");
  }
  /*
     REG_MODULE_PARAMS('sat', 'sat_simplifier_params::collect_param_descrs')
  */
  bool bce() const { return p.get_bool("bce", g, false); }
  bool abce() const { return p.get_bool("abce", g, false); }
  bool cce() const { return p.get_bool("cce", g, false); }
  bool ate() const { return p.get_bool("ate", g, true); }
  bool acce() const { return p.get_bool("acce", g, false); }
  unsigned bce_at() const { return p.get_uint("bce_at", g, 2u); }
  bool bca() const { return p.get_bool("bca", g, false); }
  unsigned bce_delay() const { return p.get_uint("bce_delay", g, 2u); }
  bool retain_blocked_clauses() const { return p.get_bool("retain_blocked_clauses", g, true); }
  unsigned blocked_clause_limit() const { return p.get_uint("blocked_clause_limit", g, 100000000u); }
  bool override_incremental() const { return p.get_bool("override_incremental", g, false); }
  unsigned resolution_limit() const { return p.get_uint("resolution.limit", g, 500000000u); }
  unsigned resolution_occ_cutoff() const { return p.get_uint("resolution.occ_cutoff", g, 10u); }
  unsigned resolution_occ_cutoff_range1() const { return p.get_uint("resolution.occ_cutoff_range1", g, 8u); }
  unsigned resolution_occ_cutoff_range2() const { return p.get_uint("resolution.occ_cutoff_range2", g, 5u); }
  unsigned resolution_occ_cutoff_range3() const { return p.get_uint("resolution.occ_cutoff_range3", g, 3u); }
  unsigned resolution_lit_cutoff_range1() const { return p.get_uint("resolution.lit_cutoff_range1", g, 700u); }
  unsigned resolution_lit_cutoff_range2() const { return p.get_uint("resolution.lit_cutoff_range2", g, 400u); }
  unsigned resolution_lit_cutoff_range3() const { return p.get_uint("resolution.lit_cutoff_range3", g, 300u); }
  unsigned resolution_cls_cutoff1() const { return p.get_uint("resolution.cls_cutoff1", g, 100000000u); }
  unsigned resolution_cls_cutoff2() const { return p.get_uint("resolution.cls_cutoff2", g, 700000000u); }
  bool elim_vars() const { return p.get_bool("elim_vars", g, true); }
  bool probing() const { return p.get_bool("probing", g, true); }
  unsigned probing_limit() const { return p.get_uint("probing_limit", g, 5000000u); }
  bool probing_cache() const { return p.get_bool("probing_cache", g, true); }
  unsigned probing_cache_limit() const { return p.get_uint("probing_cache_limit", g, 1024u); }
  bool probing_binary() const { return p.get_bool("probing_binary", g, true); }
  bool subsumption() const { return p.get_bool("subsumption", g, true); }
  unsigned subsumption_limit() const { return p.get_uint("subsumption.limit", g, 100000000u); }
};
