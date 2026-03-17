// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct opt_params {
  params_ref const & p;
  params_ref g;
  opt_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("opt")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("optsmt_engine", CPK_SYMBOL, "select optimization engine: 'basic', 'symba'", "basic","opt");
    d.insert("maxsat_engine", CPK_SYMBOL, "select engine for maxsat: 'core_maxsat', 'wmax', 'maxres', 'pd-maxres', 'maxres-bin', 'rc2'", "maxres","opt");
    d.insert("priority", CPK_SYMBOL, "select how to prioritize objectives: 'lex' (lexicographic), 'pareto', 'box'", "lex","opt");
    d.insert("dump_benchmarks", CPK_BOOL, "dump benchmarks for profiling", "false","opt");
    d.insert("dump_models", CPK_BOOL, "display intermediary models to stdout", "false","opt");
    d.insert("solution_prefix", CPK_SYMBOL, "path prefix to dump intermediary, but non-optimal, solutions", "","opt");
    d.insert("timeout", CPK_UINT, "timeout (in milliseconds) (UINT_MAX and 0 mean no timeout)", "4294967295","opt");
    d.insert("rlimit", CPK_UINT, "resource limit (0 means no limit)", "0","opt");
    d.insert("enable_sls", CPK_BOOL, "enable SLS tuning during weighted maxsat", "false","opt");
    d.insert("enable_lns", CPK_BOOL, "enable LNS during weighted maxsat", "false","opt");
    d.insert("lns_conflicts", CPK_UINT, "initial conflict count for LNS search", "1000","opt");
    d.insert("enable_core_rotate", CPK_BOOL, "enable core rotation to both sample cores and correction sets", "false","opt");
    d.insert("enable_sat", CPK_BOOL, "enable the new SAT core for propositional constraints", "true","opt");
    d.insert("elim_01", CPK_BOOL, "eliminate 01 variables", "true","opt");
    d.insert("incremental", CPK_BOOL, "set incremental mode. It disables pre-processing and enables adding constraints in model event handler", "false","opt");
    d.insert("pp.neat", CPK_BOOL, "use neat (as opposed to less readable, but faster) pretty printer when displaying context", "true","opt");
    d.insert("pb.compile_equality", CPK_BOOL, "compile arithmetical equalities into pseudo-Boolean equality (instead of two inequalites)", "false","opt");
    d.insert("pp.wcnf", CPK_BOOL, "print maxsat benchmark into wcnf format", "false","opt");
    d.insert("maxlex.enable", CPK_BOOL, "enable maxlex heuristic for lexicographic MaxSAT problems", "true","opt");
    d.insert("rc2.totalizer", CPK_BOOL, "use totalizer for rc2 encoding", "true","opt");
    d.insert("maxres.hill_climb", CPK_BOOL, "give preference for large weight cores", "true","opt");
    d.insert("maxres.add_upper_bound_block", CPK_BOOL, "restrict upper bound with constraint", "false","opt");
    d.insert("maxres.max_num_cores", CPK_UINT, "maximal number of cores per round", "200","opt");
    d.insert("maxres.max_core_size", CPK_UINT, "break batch of generated cores if size reaches this number", "3","opt");
    d.insert("maxres.maximize_assignment", CPK_BOOL, "find an MSS/MCS to improve current assignment", "false","opt");
    d.insert("maxres.max_correction_set_size", CPK_UINT, "allow generating correction set constraints up to maximal size", "3","opt");
    d.insert("maxres.wmax", CPK_BOOL, "use weighted theory solver to constrain upper bounds", "false","opt");
    d.insert("maxres.pivot_on_correction_set", CPK_BOOL, "reduce soft constraints if the current correction set is smaller than current core", "true","opt");
  }
  /*
     REG_MODULE_PARAMS('opt', 'opt_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('opt', 'optimization parameters')
  */
  symbol optsmt_engine() const { return p.get_sym("optsmt_engine", g, symbol("basic")); }
  symbol maxsat_engine() const { return p.get_sym("maxsat_engine", g, symbol("maxres")); }
  symbol priority() const { return p.get_sym("priority", g, symbol("lex")); }
  bool dump_benchmarks() const { return p.get_bool("dump_benchmarks", g, false); }
  bool dump_models() const { return p.get_bool("dump_models", g, false); }
  symbol solution_prefix() const { return p.get_sym("solution_prefix", g, symbol("")); }
  unsigned timeout() const { return p.get_uint("timeout", g, 4294967295u); }
  unsigned rlimit() const { return p.get_uint("rlimit", g, 0u); }
  bool enable_sls() const { return p.get_bool("enable_sls", g, false); }
  bool enable_lns() const { return p.get_bool("enable_lns", g, false); }
  unsigned lns_conflicts() const { return p.get_uint("lns_conflicts", g, 1000u); }
  bool enable_core_rotate() const { return p.get_bool("enable_core_rotate", g, false); }
  bool enable_sat() const { return p.get_bool("enable_sat", g, true); }
  bool elim_01() const { return p.get_bool("elim_01", g, true); }
  bool incremental() const { return p.get_bool("incremental", g, false); }
  bool pp_neat() const { return p.get_bool("pp.neat", g, true); }
  bool pb_compile_equality() const { return p.get_bool("pb.compile_equality", g, false); }
  bool pp_wcnf() const { return p.get_bool("pp.wcnf", g, false); }
  bool maxlex_enable() const { return p.get_bool("maxlex.enable", g, true); }
  bool rc2_totalizer() const { return p.get_bool("rc2.totalizer", g, true); }
  bool maxres_hill_climb() const { return p.get_bool("maxres.hill_climb", g, true); }
  bool maxres_add_upper_bound_block() const { return p.get_bool("maxres.add_upper_bound_block", g, false); }
  unsigned maxres_max_num_cores() const { return p.get_uint("maxres.max_num_cores", g, 200u); }
  unsigned maxres_max_core_size() const { return p.get_uint("maxres.max_core_size", g, 3u); }
  bool maxres_maximize_assignment() const { return p.get_bool("maxres.maximize_assignment", g, false); }
  unsigned maxres_max_correction_set_size() const { return p.get_uint("maxres.max_correction_set_size", g, 3u); }
  bool maxres_wmax() const { return p.get_bool("maxres.wmax", g, false); }
  bool maxres_pivot_on_correction_set() const { return p.get_bool("maxres.pivot_on_correction_set", g, true); }
};
