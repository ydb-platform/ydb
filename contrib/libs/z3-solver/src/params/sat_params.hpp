// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct sat_params {
  params_ref const & p;
  params_ref g;
  sat_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("sat")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("max_memory", CPK_UINT, "maximum amount of memory in megabytes", "4294967295","sat");
    d.insert("phase", CPK_SYMBOL, "phase selection strategy: always_false, always_true, basic_caching, random, caching, local_search", "caching","sat");
    d.insert("phase.sticky", CPK_BOOL, "use sticky phase caching", "true","sat");
    d.insert("search.unsat.conflicts", CPK_UINT, "period for solving for unsat (in number of conflicts)", "400","sat");
    d.insert("search.sat.conflicts", CPK_UINT, "period for solving for sat (in number of conflicts)", "400","sat");
    d.insert("rephase.base", CPK_UINT, "number of conflicts per rephase ", "1000","sat");
    d.insert("reorder.base", CPK_UINT, "number of conflicts per random reorder ", "4294967295","sat");
    d.insert("reorder.itau", CPK_DOUBLE, "inverse temperature for softmax", "4.0","sat");
    d.insert("reorder.activity_scale", CPK_UINT, "scaling factor for activity update", "100","sat");
    d.insert("propagate.prefetch", CPK_BOOL, "prefetch watch lists for assigned literals", "true","sat");
    d.insert("restart", CPK_SYMBOL, "restart strategy: static, luby, ema or geometric", "ema","sat");
    d.insert("restart.initial", CPK_UINT, "initial restart (number of conflicts)", "2","sat");
    d.insert("restart.max", CPK_UINT, "maximal number of restarts.", "4294967295","sat");
    d.insert("restart.fast", CPK_BOOL, "use fast restart approach only removing less active literals.", "true","sat");
    d.insert("restart.factor", CPK_DOUBLE, "restart increment factor for geometric strategy", "1.5","sat");
    d.insert("restart.margin", CPK_DOUBLE, "margin between fast and slow restart factors. For ema", "1.1","sat");
    d.insert("restart.emafastglue", CPK_DOUBLE, "ema alpha factor for fast moving average", "0.03","sat");
    d.insert("restart.emaslowglue", CPK_DOUBLE, "ema alpha factor for slow moving average", "1e-05","sat");
    d.insert("variable_decay", CPK_UINT, "multiplier (divided by 100) for the VSIDS activity increment", "110","sat");
    d.insert("inprocess.max", CPK_UINT, "maximal number of inprocessing passes", "4294967295","sat");
    d.insert("inprocess.out", CPK_SYMBOL, "file to dump result of the first inprocessing step and exit", "","sat");
    d.insert("branching.heuristic", CPK_SYMBOL, "branching heuristic vsids, chb", "vsids","sat");
    d.insert("branching.anti_exploration", CPK_BOOL, "apply anti-exploration heuristic for branch selection", "false","sat");
    d.insert("random_freq", CPK_DOUBLE, "frequency of random case splits", "0.01","sat");
    d.insert("random_seed", CPK_UINT, "random seed", "0","sat");
    d.insert("burst_search", CPK_UINT, "number of conflicts before first global simplification", "100","sat");
    d.insert("enable_pre_simplify", CPK_BOOL, "enable pre simplifications before the bounded search", "false","sat");
    d.insert("max_conflicts", CPK_UINT, "maximum number of conflicts", "4294967295","sat");
    d.insert("gc", CPK_SYMBOL, "garbage collection strategy: psm, glue, glue_psm, dyn_psm", "glue_psm","sat");
    d.insert("gc.initial", CPK_UINT, "learned clauses garbage collection frequency", "20000","sat");
    d.insert("gc.increment", CPK_UINT, "increment to the garbage collection threshold", "500","sat");
    d.insert("gc.small_lbd", CPK_UINT, "learned clauses with small LBD are never deleted (only used in dyn_psm)", "3","sat");
    d.insert("gc.k", CPK_UINT, "learned clauses that are inactive for k gc rounds are permanently deleted (only used in dyn_psm)", "7","sat");
    d.insert("gc.burst", CPK_BOOL, "perform eager garbage collection during initialization", "false","sat");
    d.insert("gc.defrag", CPK_BOOL, "defragment clauses when garbage collecting", "true","sat");
    d.insert("simplify.delay", CPK_UINT, "set initial delay of simplification by a conflict count", "0","sat");
    d.insert("force_cleanup", CPK_BOOL, "force cleanup to remove tautologies and simplify clauses", "false","sat");
    d.insert("minimize_lemmas", CPK_BOOL, "minimize learned clauses", "true","sat");
    d.insert("dyn_sub_res", CPK_BOOL, "dynamic subsumption resolution for minimizing learned clauses", "true","sat");
    d.insert("core.minimize", CPK_BOOL, "minimize computed core", "false","sat");
    d.insert("core.minimize_partial", CPK_BOOL, "apply partial (cheap) core minimization", "false","sat");
    d.insert("backtrack.scopes", CPK_UINT, "number of scopes to enable chronological backtracking", "100","sat");
    d.insert("backtrack.conflicts", CPK_UINT, "number of conflicts before enabling chronological backtracking", "4000","sat");
    d.insert("threads", CPK_UINT, "number of parallel threads to use", "1","sat");
    d.insert("dimacs.core", CPK_BOOL, "extract core from DIMACS benchmarks", "false","sat");
    d.insert("drat.disable", CPK_BOOL, "override anything that enables DRAT", "false","sat");
    d.insert("smt", CPK_BOOL, "use the SAT solver based incremental SMT core", "false","sat");
    d.insert("smt.proof.check", CPK_BOOL, "check proofs on the fly during SMT search", "false","sat");
    d.insert("drat.file", CPK_SYMBOL, "file to dump DRAT proofs", "","sat");
    d.insert("drat.binary", CPK_BOOL, "use Binary DRAT output format", "false","sat");
    d.insert("drat.check_unsat", CPK_BOOL, "build up internal proof and check", "false","sat");
    d.insert("drat.check_sat", CPK_BOOL, "build up internal trace, check satisfying model", "false","sat");
    d.insert("drat.activity", CPK_BOOL, "dump variable activities", "false","sat");
    d.insert("cardinality.solver", CPK_BOOL, "use cardinality solver", "true","sat");
    d.insert("pb.solver", CPK_SYMBOL, "method for handling Pseudo-Boolean constraints: circuit (arithmetical circuit), sorting (sorting circuit), totalizer (use totalizer encoding), binary_merge, segmented, solver (use native solver)", "solver","sat");
    d.insert("pb.min_arity", CPK_UINT, "minimal arity to compile pb/cardinality constraints to CNF", "9","sat");
    d.insert("cardinality.encoding", CPK_SYMBOL, "encoding used for at-most-k constraints: grouped, bimander, ordered, unate, circuit", "grouped","sat");
    d.insert("pb.resolve", CPK_SYMBOL, "resolution strategy for boolean algebra solver: cardinality, rounding", "cardinality","sat");
    d.insert("pb.lemma_format", CPK_SYMBOL, "generate either cardinality or pb lemmas", "cardinality","sat");
    d.insert("euf", CPK_BOOL, "enable euf solver (this feature is preliminary and not ready for general consumption)", "false","sat");
    d.insert("ddfw_search", CPK_BOOL, "use ddfw local search instead of CDCL", "false","sat");
    d.insert("ddfw.init_clause_weight", CPK_UINT, "initial clause weight for DDFW local search", "8","sat");
    d.insert("ddfw.use_reward_pct", CPK_UINT, "percentage to pick highest reward variable when it has reward 0", "15","sat");
    d.insert("ddfw.restart_base", CPK_UINT, "number of flips used a starting point for hesitant restart backoff", "100000","sat");
    d.insert("ddfw.reinit_base", CPK_UINT, "increment basis for geometric backoff scheme of re-initialization of weights", "10000","sat");
    d.insert("ddfw.threads", CPK_UINT, "number of ddfw threads to run in parallel with sat solver", "0","sat");
    d.insert("prob_search", CPK_BOOL, "use probsat local search instead of CDCL", "false","sat");
    d.insert("local_search", CPK_BOOL, "use local search instead of CDCL", "false","sat");
    d.insert("local_search_threads", CPK_UINT, "number of local search threads to find satisfiable solution", "0","sat");
    d.insert("local_search_mode", CPK_SYMBOL, "local search algorithm, either default wsat or qsat", "wsat","sat");
    d.insert("local_search_dbg_flips", CPK_BOOL, "write debug information for number of flips", "false","sat");
    d.insert("anf", CPK_BOOL, "enable ANF based simplification in-processing", "false","sat");
    d.insert("anf.delay", CPK_UINT, "delay ANF simplification by in-processing round", "2","sat");
    d.insert("anf.exlin", CPK_BOOL, "enable extended linear simplification", "false","sat");
    d.insert("cut", CPK_BOOL, "enable AIG based simplification in-processing", "false","sat");
    d.insert("cut.delay", CPK_UINT, "delay cut simplification by in-processing round", "2","sat");
    d.insert("cut.aig", CPK_BOOL, "extract aigs (and ites) from cluases for cut simplification", "false","sat");
    d.insert("cut.lut", CPK_BOOL, "extract luts from clauses for cut simplification", "false","sat");
    d.insert("cut.xor", CPK_BOOL, "extract xors from clauses for cut simplification", "false","sat");
    d.insert("cut.npn3", CPK_BOOL, "extract 3 input functions from clauses for cut simplification", "false","sat");
    d.insert("cut.dont_cares", CPK_BOOL, "integrate dont cares with cuts", "true","sat");
    d.insert("cut.redundancies", CPK_BOOL, "integrate redundancy checking of cuts", "true","sat");
    d.insert("cut.force", CPK_BOOL, "force redoing cut-enumeration until a fixed-point", "false","sat");
    d.insert("lookahead.cube.cutoff", CPK_SYMBOL, "cutoff type used to create lookahead cubes: depth, freevars, psat, adaptive_freevars, adaptive_psat", "depth","sat");
    d.insert("lookahead.cube.fraction", CPK_DOUBLE, "adaptive fraction to create lookahead cubes. Used when lookahead.cube.cutoff is adaptive_freevars or adaptive_psat", "0.4","sat");
    d.insert("lookahead.cube.depth", CPK_UINT, "cut-off depth to create cubes. Used when lookahead.cube.cutoff is depth.", "1","sat");
    d.insert("lookahead.cube.freevars", CPK_DOUBLE, "cube free variable fraction. Used when lookahead.cube.cutoff is freevars", "0.8","sat");
    d.insert("lookahead.cube.psat.var_exp", CPK_DOUBLE, "free variable exponent for PSAT cutoff", "1","sat");
    d.insert("lookahead.cube.psat.clause_base", CPK_DOUBLE, "clause base for PSAT cutoff", "2","sat");
    d.insert("lookahead.cube.psat.trigger", CPK_DOUBLE, "trigger value to create lookahead cubes for PSAT cutoff. Used when lookahead.cube.cutoff is psat", "5","sat");
    d.insert("lookahead.preselect", CPK_BOOL, "use pre-selection of subset of variables for branching", "false","sat");
    d.insert("lookahead_simplify", CPK_BOOL, "use lookahead solver during simplification", "false","sat");
    d.insert("lookahead_scores", CPK_BOOL, "extract lookahead scores. A utility that can only be used from the DIMACS front-end", "false","sat");
    d.insert("lookahead.double", CPK_BOOL, "enable double lookahead", "true","sat");
    d.insert("lookahead.use_learned", CPK_BOOL, "use learned clauses when selecting lookahead literal", "false","sat");
    d.insert("lookahead_simplify.bca", CPK_BOOL, "add learned binary clauses as part of lookahead simplification", "true","sat");
    d.insert("lookahead.global_autarky", CPK_BOOL, "prefer to branch on variables that occur in clauses that are reduced", "false","sat");
    d.insert("lookahead.delta_fraction", CPK_DOUBLE, "number between 0 and 1, the smaller the more literals are selected for double lookahead", "1.0","sat");
    d.insert("lookahead.reward", CPK_SYMBOL, "select lookahead heuristic: ternary, heule_schur (Heule Schur), heuleu (Heule Unit), unit, or march_cu", "march_cu","sat");
  }
  /*
     REG_MODULE_PARAMS('sat', 'sat_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('sat', 'propositional SAT solver')
  */
  unsigned max_memory() const { return p.get_uint("max_memory", g, 4294967295u); }
  symbol phase() const { return p.get_sym("phase", g, symbol("caching")); }
  bool phase_sticky() const { return p.get_bool("phase.sticky", g, true); }
  unsigned search_unsat_conflicts() const { return p.get_uint("search.unsat.conflicts", g, 400u); }
  unsigned search_sat_conflicts() const { return p.get_uint("search.sat.conflicts", g, 400u); }
  unsigned rephase_base() const { return p.get_uint("rephase.base", g, 1000u); }
  unsigned reorder_base() const { return p.get_uint("reorder.base", g, 4294967295u); }
  double reorder_itau() const { return p.get_double("reorder.itau", g, 4.0); }
  unsigned reorder_activity_scale() const { return p.get_uint("reorder.activity_scale", g, 100u); }
  bool propagate_prefetch() const { return p.get_bool("propagate.prefetch", g, true); }
  symbol restart() const { return p.get_sym("restart", g, symbol("ema")); }
  unsigned restart_initial() const { return p.get_uint("restart.initial", g, 2u); }
  unsigned restart_max() const { return p.get_uint("restart.max", g, 4294967295u); }
  bool restart_fast() const { return p.get_bool("restart.fast", g, true); }
  double restart_factor() const { return p.get_double("restart.factor", g, 1.5); }
  double restart_margin() const { return p.get_double("restart.margin", g, 1.1); }
  double restart_emafastglue() const { return p.get_double("restart.emafastglue", g, 0.03); }
  double restart_emaslowglue() const { return p.get_double("restart.emaslowglue", g, 1e-05); }
  unsigned variable_decay() const { return p.get_uint("variable_decay", g, 110u); }
  unsigned inprocess_max() const { return p.get_uint("inprocess.max", g, 4294967295u); }
  symbol inprocess_out() const { return p.get_sym("inprocess.out", g, symbol("")); }
  symbol branching_heuristic() const { return p.get_sym("branching.heuristic", g, symbol("vsids")); }
  bool branching_anti_exploration() const { return p.get_bool("branching.anti_exploration", g, false); }
  double random_freq() const { return p.get_double("random_freq", g, 0.01); }
  unsigned random_seed() const { return p.get_uint("random_seed", g, 0u); }
  unsigned burst_search() const { return p.get_uint("burst_search", g, 100u); }
  bool enable_pre_simplify() const { return p.get_bool("enable_pre_simplify", g, false); }
  unsigned max_conflicts() const { return p.get_uint("max_conflicts", g, 4294967295u); }
  symbol gc() const { return p.get_sym("gc", g, symbol("glue_psm")); }
  unsigned gc_initial() const { return p.get_uint("gc.initial", g, 20000u); }
  unsigned gc_increment() const { return p.get_uint("gc.increment", g, 500u); }
  unsigned gc_small_lbd() const { return p.get_uint("gc.small_lbd", g, 3u); }
  unsigned gc_k() const { return p.get_uint("gc.k", g, 7u); }
  bool gc_burst() const { return p.get_bool("gc.burst", g, false); }
  bool gc_defrag() const { return p.get_bool("gc.defrag", g, true); }
  unsigned simplify_delay() const { return p.get_uint("simplify.delay", g, 0u); }
  bool force_cleanup() const { return p.get_bool("force_cleanup", g, false); }
  bool minimize_lemmas() const { return p.get_bool("minimize_lemmas", g, true); }
  bool dyn_sub_res() const { return p.get_bool("dyn_sub_res", g, true); }
  bool core_minimize() const { return p.get_bool("core.minimize", g, false); }
  bool core_minimize_partial() const { return p.get_bool("core.minimize_partial", g, false); }
  unsigned backtrack_scopes() const { return p.get_uint("backtrack.scopes", g, 100u); }
  unsigned backtrack_conflicts() const { return p.get_uint("backtrack.conflicts", g, 4000u); }
  unsigned threads() const { return p.get_uint("threads", g, 1u); }
  bool dimacs_core() const { return p.get_bool("dimacs.core", g, false); }
  bool drat_disable() const { return p.get_bool("drat.disable", g, false); }
  bool smt() const { return p.get_bool("smt", g, false); }
  bool smt_proof_check() const { return p.get_bool("smt.proof.check", g, false); }
  symbol drat_file() const { return p.get_sym("drat.file", g, symbol("")); }
  bool drat_binary() const { return p.get_bool("drat.binary", g, false); }
  bool drat_check_unsat() const { return p.get_bool("drat.check_unsat", g, false); }
  bool drat_check_sat() const { return p.get_bool("drat.check_sat", g, false); }
  bool drat_activity() const { return p.get_bool("drat.activity", g, false); }
  bool cardinality_solver() const { return p.get_bool("cardinality.solver", g, true); }
  symbol pb_solver() const { return p.get_sym("pb.solver", g, symbol("solver")); }
  unsigned pb_min_arity() const { return p.get_uint("pb.min_arity", g, 9u); }
  symbol cardinality_encoding() const { return p.get_sym("cardinality.encoding", g, symbol("grouped")); }
  symbol pb_resolve() const { return p.get_sym("pb.resolve", g, symbol("cardinality")); }
  symbol pb_lemma_format() const { return p.get_sym("pb.lemma_format", g, symbol("cardinality")); }
  bool euf() const { return p.get_bool("euf", g, false); }
  bool ddfw_search() const { return p.get_bool("ddfw_search", g, false); }
  unsigned ddfw_init_clause_weight() const { return p.get_uint("ddfw.init_clause_weight", g, 8u); }
  unsigned ddfw_use_reward_pct() const { return p.get_uint("ddfw.use_reward_pct", g, 15u); }
  unsigned ddfw_restart_base() const { return p.get_uint("ddfw.restart_base", g, 100000u); }
  unsigned ddfw_reinit_base() const { return p.get_uint("ddfw.reinit_base", g, 10000u); }
  unsigned ddfw_threads() const { return p.get_uint("ddfw.threads", g, 0u); }
  bool prob_search() const { return p.get_bool("prob_search", g, false); }
  bool local_search() const { return p.get_bool("local_search", g, false); }
  unsigned local_search_threads() const { return p.get_uint("local_search_threads", g, 0u); }
  symbol local_search_mode() const { return p.get_sym("local_search_mode", g, symbol("wsat")); }
  bool local_search_dbg_flips() const { return p.get_bool("local_search_dbg_flips", g, false); }
  bool anf() const { return p.get_bool("anf", g, false); }
  unsigned anf_delay() const { return p.get_uint("anf.delay", g, 2u); }
  bool anf_exlin() const { return p.get_bool("anf.exlin", g, false); }
  bool cut() const { return p.get_bool("cut", g, false); }
  unsigned cut_delay() const { return p.get_uint("cut.delay", g, 2u); }
  bool cut_aig() const { return p.get_bool("cut.aig", g, false); }
  bool cut_lut() const { return p.get_bool("cut.lut", g, false); }
  bool cut_xor() const { return p.get_bool("cut.xor", g, false); }
  bool cut_npn3() const { return p.get_bool("cut.npn3", g, false); }
  bool cut_dont_cares() const { return p.get_bool("cut.dont_cares", g, true); }
  bool cut_redundancies() const { return p.get_bool("cut.redundancies", g, true); }
  bool cut_force() const { return p.get_bool("cut.force", g, false); }
  symbol lookahead_cube_cutoff() const { return p.get_sym("lookahead.cube.cutoff", g, symbol("depth")); }
  double lookahead_cube_fraction() const { return p.get_double("lookahead.cube.fraction", g, 0.4); }
  unsigned lookahead_cube_depth() const { return p.get_uint("lookahead.cube.depth", g, 1u); }
  double lookahead_cube_freevars() const { return p.get_double("lookahead.cube.freevars", g, 0.8); }
  double lookahead_cube_psat_var_exp() const { return p.get_double("lookahead.cube.psat.var_exp", g, 1); }
  double lookahead_cube_psat_clause_base() const { return p.get_double("lookahead.cube.psat.clause_base", g, 2); }
  double lookahead_cube_psat_trigger() const { return p.get_double("lookahead.cube.psat.trigger", g, 5); }
  bool lookahead_preselect() const { return p.get_bool("lookahead.preselect", g, false); }
  bool lookahead_simplify() const { return p.get_bool("lookahead_simplify", g, false); }
  bool lookahead_scores() const { return p.get_bool("lookahead_scores", g, false); }
  bool lookahead_double() const { return p.get_bool("lookahead.double", g, true); }
  bool lookahead_use_learned() const { return p.get_bool("lookahead.use_learned", g, false); }
  bool lookahead_simplify_bca() const { return p.get_bool("lookahead_simplify.bca", g, true); }
  bool lookahead_global_autarky() const { return p.get_bool("lookahead.global_autarky", g, false); }
  double lookahead_delta_fraction() const { return p.get_double("lookahead.delta_fraction", g, 1.0); }
  symbol lookahead_reward() const { return p.get_sym("lookahead.reward", g, symbol("march_cu")); }
};
