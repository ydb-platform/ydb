// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct tactic_params {
  params_ref const & p;
  params_ref g;
  tactic_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("tactic")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("solve_eqs.context_solve", CPK_BOOL, "solve equalities within disjunctions.", "true","tactic");
    d.insert("solve_eqs.theory_solver", CPK_BOOL, "use theory solvers.", "true","tactic");
    d.insert("solve_eqs.ite_solver", CPK_BOOL, "use if-then-else solvers.", "true","tactic");
    d.insert("solve_eqs.max_occs", CPK_UINT, "maximum number of occurrences for considering a variable for gaussian eliminations.", "4294967295","tactic");
    d.insert("blast_term_ite.max_inflation", CPK_UINT, "multiplicative factor of initial term size.", "4294967295","tactic");
    d.insert("blast_term_ite.max_steps", CPK_UINT, "maximal number of steps allowed for tactic.", "4294967295","tactic");
    d.insert("propagate_values.max_rounds", CPK_UINT, "maximal number of rounds to propagate values.", "4","tactic");
    d.insert("lia2card.max_range", CPK_UINT, "maximal range of integers to compilation into Booleans", "100","tactic");
    d.insert("lia2card.max_ite_nesting", CPK_UINT, "maximal nesting depth for ite expressions to be compiled into PB constraints", "4","tactic");
    d.insert("randomizer.seed", CPK_UINT, "seed for randomizer pre-processor", "0","tactic");
    d.insert("default_tactic", CPK_SYMBOL, "overwrite default tactic in strategic solver", "","tactic");
  }
  /*
     REG_MODULE_PARAMS('tactic', 'tactic_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('tactic', 'tactic parameters')
  */
  bool solve_eqs_context_solve() const { return p.get_bool("solve_eqs.context_solve", g, true); }
  bool solve_eqs_theory_solver() const { return p.get_bool("solve_eqs.theory_solver", g, true); }
  bool solve_eqs_ite_solver() const { return p.get_bool("solve_eqs.ite_solver", g, true); }
  unsigned solve_eqs_max_occs() const { return p.get_uint("solve_eqs.max_occs", g, 4294967295u); }
  unsigned blast_term_ite_max_inflation() const { return p.get_uint("blast_term_ite.max_inflation", g, 4294967295u); }
  unsigned blast_term_ite_max_steps() const { return p.get_uint("blast_term_ite.max_steps", g, 4294967295u); }
  unsigned propagate_values_max_rounds() const { return p.get_uint("propagate_values.max_rounds", g, 4u); }
  unsigned lia2card_max_range() const { return p.get_uint("lia2card.max_range", g, 100u); }
  unsigned lia2card_max_ite_nesting() const { return p.get_uint("lia2card.max_ite_nesting", g, 4u); }
  unsigned randomizer_seed() const { return p.get_uint("randomizer.seed", g, 0u); }
  symbol default_tactic() const { return p.get_sym("default_tactic", g, symbol("")); }
};
