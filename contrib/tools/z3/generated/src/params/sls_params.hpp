// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct sls_params {
  params_ref const & p;
  params_ref g;
  sls_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("sls")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("max_memory", CPK_UINT, "maximum amount of memory in megabytes", "4294967295","sls");
    d.insert("max_restarts", CPK_UINT, "maximum number of restarts", "4294967295","sls");
    d.insert("max_repairs", CPK_UINT, "maximum number of repairs before restart", "1000","sls");
    d.insert("walksat", CPK_BOOL, "use walksat assertion selection (instead of gsat)", "true","sls");
    d.insert("walksat_ucb", CPK_BOOL, "use bandit heuristic for walksat assertion selection (instead of random)", "true","sls");
    d.insert("walksat_ucb_constant", CPK_DOUBLE, "the ucb constant c in the term score + c * f(touched)", "20.0","sls");
    d.insert("walksat_ucb_init", CPK_BOOL, "initialize total ucb touched to formula size", "false","sls");
    d.insert("walksat_ucb_forget", CPK_DOUBLE, "scale touched by this factor every base restart interval", "1.0","sls");
    d.insert("walksat_ucb_noise", CPK_DOUBLE, "add noise 0 <= 256 * ucb_noise to ucb score for assertion selection", "0.0002","sls");
    d.insert("walksat_repick", CPK_BOOL, "repick assertion if randomizing in local minima", "true","sls");
    d.insert("scale_unsat", CPK_DOUBLE, "scale score of unsat expressions by this factor", "0.5","sls");
    d.insert("paws_init", CPK_UINT, "initial/minimum assertion weights", "40","sls");
    d.insert("paws_sp", CPK_UINT, "smooth assertion weights with probability paws_sp / 1024", "52","sls");
    d.insert("wp", CPK_UINT, "random walk with probability wp / 1024", "100","sls");
    d.insert("vns_mc", CPK_UINT, "in local minima, try Monte Carlo sampling vns_mc many 2-bit-flips per bit", "0","sls");
    d.insert("vns_repick", CPK_BOOL, "in local minima, try picking a different assertion (only for walksat)", "false","sls");
    d.insert("restart_base", CPK_UINT, "base restart interval given by moves per run", "100","sls");
    d.insert("restart_init", CPK_BOOL, "initialize to 0 or random value (= 1) after restart", "false","sls");
    d.insert("early_prune", CPK_BOOL, "use early pruning for score prediction", "true","sls");
    d.insert("random_offset", CPK_BOOL, "use random offset for candidate evaluation", "true","sls");
    d.insert("rescore", CPK_BOOL, "rescore/normalize top-level score every base restart interval", "true","sls");
    d.insert("dt_axiomatic", CPK_BOOL, "use axiomatic mode or model reduction for datatype solver", "true","sls");
    d.insert("track_unsat", CPK_BOOL, "keep a list of unsat assertions as done in SAT - currently disabled internally", "false","sls");
    d.insert("random_seed", CPK_UINT, "random seed", "0","sls");
    d.insert("arith_use_lookahead", CPK_BOOL, "use lookahead solver for NIRA", "true","sls");
    d.insert("arith_allow_plateau", CPK_BOOL, "allow plateau moves during NIRA solving", "false","sls");
    d.insert("arith_use_clausal_lookahead", CPK_BOOL, "use clause based lookahead for NIRA", "false","sls");
    d.insert("bv_use_top_level_assertions", CPK_BOOL, "use top-level assertions for BV lookahead solver", "true","sls");
    d.insert("bv_use_lookahead", CPK_BOOL, "use lookahead solver for BV", "true","sls");
    d.insert("bv_allow_rotation", CPK_BOOL, "allow model rotation when repairing literal assignment", "true","sls");
    d.insert("str_update_strategy", CPK_UINT, "string update candidate selection: 0 - single character based update, 1 - subsequence based update, 2 - combined", "2","sls");
  }
  /*
     REG_MODULE_PARAMS('sls', 'sls_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('sls', 'Stochastic Local Search Solver (invoked by sls-qfbv and sls-smt tactics or enabled by smt.sls.enable=true)')
  */
  unsigned max_memory() const { return p.get_uint("max_memory", g, 4294967295u); }
  unsigned max_restarts() const { return p.get_uint("max_restarts", g, 4294967295u); }
  unsigned max_repairs() const { return p.get_uint("max_repairs", g, 1000u); }
  bool walksat() const { return p.get_bool("walksat", g, true); }
  bool walksat_ucb() const { return p.get_bool("walksat_ucb", g, true); }
  double walksat_ucb_constant() const { return p.get_double("walksat_ucb_constant", g, 20.0); }
  bool walksat_ucb_init() const { return p.get_bool("walksat_ucb_init", g, false); }
  double walksat_ucb_forget() const { return p.get_double("walksat_ucb_forget", g, 1.0); }
  double walksat_ucb_noise() const { return p.get_double("walksat_ucb_noise", g, 0.0002); }
  bool walksat_repick() const { return p.get_bool("walksat_repick", g, true); }
  double scale_unsat() const { return p.get_double("scale_unsat", g, 0.5); }
  unsigned paws_init() const { return p.get_uint("paws_init", g, 40u); }
  unsigned paws_sp() const { return p.get_uint("paws_sp", g, 52u); }
  unsigned wp() const { return p.get_uint("wp", g, 100u); }
  unsigned vns_mc() const { return p.get_uint("vns_mc", g, 0u); }
  bool vns_repick() const { return p.get_bool("vns_repick", g, false); }
  unsigned restart_base() const { return p.get_uint("restart_base", g, 100u); }
  bool restart_init() const { return p.get_bool("restart_init", g, false); }
  bool early_prune() const { return p.get_bool("early_prune", g, true); }
  bool random_offset() const { return p.get_bool("random_offset", g, true); }
  bool rescore() const { return p.get_bool("rescore", g, true); }
  bool dt_axiomatic() const { return p.get_bool("dt_axiomatic", g, true); }
  bool track_unsat() const { return p.get_bool("track_unsat", g, false); }
  unsigned random_seed() const { return p.get_uint("random_seed", g, 0u); }
  bool arith_use_lookahead() const { return p.get_bool("arith_use_lookahead", g, true); }
  bool arith_allow_plateau() const { return p.get_bool("arith_allow_plateau", g, false); }
  bool arith_use_clausal_lookahead() const { return p.get_bool("arith_use_clausal_lookahead", g, false); }
  bool bv_use_top_level_assertions() const { return p.get_bool("bv_use_top_level_assertions", g, true); }
  bool bv_use_lookahead() const { return p.get_bool("bv_use_lookahead", g, true); }
  bool bv_allow_rotation() const { return p.get_bool("bv_allow_rotation", g, true); }
  unsigned str_update_strategy() const { return p.get_uint("str_update_strategy", g, 2u); }
};
