// Automatically generated file.
#include "tactic/tactic.h"
#include "cmd_context/tactic_cmds.h"
#include "cmd_context/simplifier_cmds.h"
#include "cmd_context/cmd_context.h"
#include "ackermannization/ackermannize_bv_tactic.h"
#include "ackermannization/ackr_bound_probe.h"
#include "ast/simplifiers/bit2int.h"
#include "ast/simplifiers/bit_blaster.h"
#include "ast/simplifiers/elim_bounds.h"
#include "ast/simplifiers/elim_term_ite.h"
#include "ast/simplifiers/pull_nested_quantifiers.h"
#include "ast/simplifiers/push_ite.h"
#include "ast/simplifiers/randomizer.h"
#include "ast/simplifiers/refine_inj_axiom.h"
#include "ast/simplifiers/rewriter_simplifier.h"
#include "math/subpaving/tactic/subpaving_tactic.h"
#include "muz/fp/horn_tactic.h"
#include "nlsat/tactic/nlsat_tactic.h"
#include "nlsat/tactic/qfnra_nlsat_tactic.h"
#include "qe/lite/qe_lite_tactic.h"
#include "qe/nlqsat.h"
#include "qe/qe_tactic.h"
#include "qe/qsat.h"
#include "sat/sat_solver/inc_sat_solver.h"
#include "sat/tactic/sat_tactic.h"
#include "smt/tactic/ctx_solver_simplify_tactic.h"
#include "smt/tactic/smt_tactic_core.h"
#include "smt/tactic/unit_subsumption_tactic.h"
#include "tactic/aig/aig_tactic.h"
#include "tactic/arith/add_bounds_tactic.h"
#include "tactic/arith/card2bv_tactic.h"
#include "tactic/arith/degree_shift_tactic.h"
#include "tactic/arith/diff_neq_tactic.h"
#include "tactic/arith/eq2bv_tactic.h"
#include "tactic/arith/factor_tactic.h"
#include "tactic/arith/fix_dl_var_tactic.h"
#include "tactic/arith/fm_tactic.h"
#include "tactic/arith/lia2card_tactic.h"
#include "tactic/arith/lia2pb_tactic.h"
#include "tactic/arith/nla2bv_tactic.h"
#include "tactic/arith/normalize_bounds_tactic.h"
#include "tactic/arith/pb2bv_tactic.h"
#include "tactic/arith/probe_arith.h"
#include "tactic/arith/propagate_ineqs_tactic.h"
#include "tactic/arith/purify_arith_tactic.h"
#include "tactic/arith/recover_01_tactic.h"
#include "tactic/bv/bit_blaster_tactic.h"
#include "tactic/bv/bv1_blaster_tactic.h"
#include "tactic/bv/bv_bound_chk_tactic.h"
#include "tactic/bv/bv_bounds_tactic.h"
#include "tactic/bv/bv_size_reduction_tactic.h"
#include "tactic/bv/bv_slice_tactic.h"
#include "tactic/bv/bvarray2uf_tactic.h"
#include "tactic/bv/dt2bv_tactic.h"
#include "tactic/bv/elim_small_bv_tactic.h"
#include "tactic/bv/max_bv_sharing_tactic.h"
#include "tactic/core/blast_term_ite_tactic.h"
#include "tactic/core/cofactor_term_ite_tactic.h"
#include "tactic/core/collect_statistics_tactic.h"
#include "tactic/core/ctx_simplify_tactic.h"
#include "tactic/core/demodulator_tactic.h"
#include "tactic/core/der_tactic.h"
#include "tactic/core/distribute_forall_tactic.h"
#include "tactic/core/dom_simplify_tactic.h"
#include "tactic/core/elim_term_ite_tactic.h"
#include "tactic/core/elim_uncnstr2_tactic.h"
#include "tactic/core/elim_uncnstr_tactic.h"
#include "tactic/core/eliminate_predicates_tactic.h"
#include "tactic/core/injectivity_tactic.h"
#include "tactic/core/nnf_tactic.h"
#include "tactic/core/occf_tactic.h"
#include "tactic/core/pb_preprocess_tactic.h"
#include "tactic/core/propagate_values2_tactic.h"
#include "tactic/core/propagate_values_tactic.h"
#include "tactic/core/reduce_args_tactic.h"
#include "tactic/core/simplify_tactic.h"
#include "tactic/core/solve_eqs_tactic.h"
#include "tactic/core/special_relations_tactic.h"
#include "tactic/core/split_clause_tactic.h"
#include "tactic/core/symmetry_reduce_tactic.h"
#include "tactic/core/tseitin_cnf_tactic.h"
#include "tactic/fd_solver/fd_solver.h"
#include "tactic/fd_solver/smtfd_solver.h"
#include "tactic/fpa/fpa2bv_tactic.h"
#include "tactic/fpa/qffp_tactic.h"
#include "tactic/fpa/qffplra_tactic.h"
#include "tactic/portfolio/default_tactic.h"
#include "tactic/portfolio/euf_completion_tactic.h"
#include "tactic/portfolio/solver_subsumption_tactic.h"
#include "tactic/probe.h"
#include "tactic/sls/sls_tactic.h"
#include "tactic/smtlogics/nra_tactic.h"
#include "tactic/smtlogics/qfaufbv_tactic.h"
#include "tactic/smtlogics/qfauflia_tactic.h"
#include "tactic/smtlogics/qfbv_tactic.h"
#include "tactic/smtlogics/qfidl_tactic.h"
#include "tactic/smtlogics/qflia_tactic.h"
#include "tactic/smtlogics/qflra_tactic.h"
#include "tactic/smtlogics/qfnia_tactic.h"
#include "tactic/smtlogics/qfnra_tactic.h"
#include "tactic/smtlogics/qfuf_tactic.h"
#include "tactic/smtlogics/qfufbv_tactic.h"
#include "tactic/smtlogics/quant_tactics.h"
#include "tactic/smtlogics/smt_tactic.h"
#include "tactic/tactic.h"
#include "tactic/ufbv/macro_finder_tactic.h"
#include "tactic/ufbv/quasi_macros_tactic.h"
#include "tactic/ufbv/ufbv_rewriter_tactic.h"
#include "tactic/ufbv/ufbv_tactic.h"
#define ADD_TACTIC_CMD(NAME, DESCR, CODE) ctx.insert(alloc(tactic_cmd, symbol(NAME), DESCR, [](ast_manager &m, const params_ref &p) { return CODE; }))
#define ADD_PROBE(NAME, DESCR, PROBE) ctx.insert(alloc(probe_info, symbol(NAME), DESCR, PROBE))
#define ADD_SIMPLIFIER_CMD(NAME, DESCR, CODE) ctx.insert(alloc(simplifier_cmd, symbol(NAME), DESCR, [](auto& m, auto& p, auto &s) -> dependent_expr_simplifier* { return CODE; }))
void install_tactics(tactic_manager & ctx) {
  ADD_TACTIC_CMD("ackermannize_bv", "A tactic for performing full Ackermannization on bv instances.", mk_ackermannize_bv_tactic(m, p));
  ADD_TACTIC_CMD("subpaving", "tactic for testing subpaving module.", mk_subpaving_tactic(m, p));
  ADD_TACTIC_CMD("horn", "apply tactic for horn clauses.", mk_horn_tactic(m, p));
  ADD_TACTIC_CMD("horn-simplify", "simplify horn clauses.", mk_horn_simplify_tactic(m, p));
  ADD_TACTIC_CMD("nlsat", "(try to) solve goal using a nonlinear arithmetic solver.", mk_nlsat_tactic(m, p));
  ADD_TACTIC_CMD("qfnra-nlsat", "builtin strategy for solving QF_NRA problems using only nlsat.", mk_qfnra_nlsat_tactic(m, p));
  ADD_TACTIC_CMD("qe-light", "apply light-weight quantifier elimination.", mk_qe_lite_tactic(m, p));
  ADD_TACTIC_CMD("nlqsat", "apply a NL-QSAT solver.", mk_nlqsat_tactic(m, p));
  ADD_TACTIC_CMD("qe", "apply quantifier elimination.", mk_qe_tactic(m, p));
  ADD_TACTIC_CMD("qsat", "apply a QSAT solver.", mk_qsat_tactic(m, p));
  ADD_TACTIC_CMD("qe2", "apply a QSAT based quantifier elimination.", mk_qe2_tactic(m, p));
  ADD_TACTIC_CMD("qe_rec", "apply a QSAT based quantifier elimination recursively.", mk_qe_rec_tactic(m, p));
  ADD_TACTIC_CMD("psat", "(try to) solve goal using a parallel SAT solver.", mk_psat_tactic(m, p));
  ADD_TACTIC_CMD("sat", "(try to) solve goal using a SAT solver.", mk_sat_tactic(m, p));
  ADD_TACTIC_CMD("sat-preprocess", "Apply SAT solver preprocessing procedures (bounded resolution, Boolean constant propagation, 2-SAT, subsumption, subsumption resolution).", mk_sat_preprocessor_tactic(m, p));
  ADD_TACTIC_CMD("ctx-solver-simplify", "apply solver-based contextual simplification rules.", mk_ctx_solver_simplify_tactic(m, p));
  ADD_TACTIC_CMD("psmt", "builtin strategy for SMT tactic in parallel.", mk_parallel_smt_tactic(m, p));
  ADD_TACTIC_CMD("unit-subsume-simplify", "unit subsumption simplification.", mk_unit_subsumption_tactic(m, p));
  ADD_TACTIC_CMD("aig", "simplify Boolean structure using AIGs.", mk_aig_tactic());
  ADD_TACTIC_CMD("add-bounds", "add bounds to unbounded variables (under approximation).", mk_add_bounds_tactic(m, p));
  ADD_TACTIC_CMD("card2bv", "convert pseudo-boolean constraints to bit-vectors.", mk_card2bv_tactic(m, p));
  ADD_TACTIC_CMD("degree-shift", "try to reduce degree of polynomials (remark: :mul2power simplification is automatically applied).", mk_degree_shift_tactic(m, p));
  ADD_TACTIC_CMD("diff-neq", "specialized solver for integer arithmetic problems that contain only atoms of the form (<= k x) (<= x k) and (not (= (- x y) k)), where x and y are constants and k is a numeral, and all constants are bounded.", mk_diff_neq_tactic(m, p));
  ADD_TACTIC_CMD("eq2bv", "convert integer variables used as finite domain elements to bit-vectors.", mk_eq2bv_tactic(m));
  ADD_TACTIC_CMD("factor", "polynomial factorization.", mk_factor_tactic(m, p));
  ADD_TACTIC_CMD("fix-dl-var", "if goal is in the difference logic fragment, then fix the variable with the most number of occurrences at 0.", mk_fix_dl_var_tactic(m, p));
  ADD_TACTIC_CMD("fm", "eliminate variables using fourier-motzkin elimination.", mk_fm_tactic(m, p));
  ADD_TACTIC_CMD("lia2card", "introduce cardinality constraints from 0-1 integer.", mk_lia2card_tactic(m, p));
  ADD_TACTIC_CMD("lia2pb", "convert bounded integer variables into a sequence of 0-1 variables.", mk_lia2pb_tactic(m, p));
  ADD_TACTIC_CMD("nla2bv", "convert a nonlinear arithmetic problem into a bit-vector problem, in most cases the resultant goal is an under approximation and is useul for finding models.", mk_nla2bv_tactic(m, p));
  ADD_TACTIC_CMD("normalize-bounds", "replace a variable x with lower bound k <= x with x' = x - k.", mk_normalize_bounds_tactic(m, p));
  ADD_TACTIC_CMD("pb2bv", "convert pseudo-boolean constraints to bit-vectors.", mk_pb2bv_tactic(m, p));
  ADD_TACTIC_CMD("propagate-ineqs", "propagate ineqs/bounds, remove subsumed inequalities.", mk_propagate_ineqs_tactic(m, p));
  ADD_TACTIC_CMD("purify-arith", "eliminate unnecessary operators: -, /, div, mod, rem, is-int, to-int, ^, root-objects.", mk_purify_arith_tactic(m, p));
  ADD_TACTIC_CMD("recover-01", "recover 0-1 variables hidden as Boolean variables.", mk_recover_01_tactic(m, p));
  ADD_TACTIC_CMD("bit-blast", "reduce bit-vector expressions into SAT.", mk_bit_blaster_tactic(m, p));
  ADD_TACTIC_CMD("bv1-blast", "reduce bit-vector expressions into bit-vectors of size 1 (notes: only equality, extract and concat are supported).", mk_bv1_blaster_tactic(m, p));
  ADD_TACTIC_CMD("bv_bound_chk", "attempts to detect inconsistencies of bounds on bv expressions.", mk_bv_bound_chk_tactic(m, p));
  ADD_TACTIC_CMD("propagate-bv-bounds", "propagate bit-vector bounds by simplifying implied or contradictory bounds.", mk_bv_bounds_tactic(m, p));
  ADD_TACTIC_CMD("propagate-bv-bounds2", "propagate bit-vector bounds by simplifying implied or contradictory bounds.", mk_dom_bv_bounds_tactic(m, p));
  ADD_TACTIC_CMD("reduce-bv-size", "try to reduce bit-vector sizes using inequalities.", mk_bv_size_reduction_tactic(m, p));
  ADD_TACTIC_CMD("bv-slice", "simplify using bit-vector slices.", mk_bv_slice_tactic(m, p));
  ADD_TACTIC_CMD("bvarray2uf", "Rewrite bit-vector arrays into bit-vector (uninterpreted) functions.", mk_bvarray2uf_tactic(m, p));
  ADD_TACTIC_CMD("dt2bv", "eliminate finite domain data-types. Replace by bit-vectors.", mk_dt2bv_tactic(m, p));
  ADD_TACTIC_CMD("elim-small-bv", "eliminate small, quantified bit-vectors by expansion.", mk_elim_small_bv_tactic(m, p));
  ADD_TACTIC_CMD("max-bv-sharing", "use heuristics to maximize the sharing of bit-vector expressions such as adders and multipliers.", mk_max_bv_sharing_tactic(m, p));
  ADD_TACTIC_CMD("blast-term-ite", "blast term if-then-else by hoisting them.", mk_blast_term_ite_tactic(m, p));
  ADD_TACTIC_CMD("cofactor-term-ite", "eliminate term if-the-else using cofactors.", mk_cofactor_term_ite_tactic(m, p));
  ADD_TACTIC_CMD("collect-statistics", "Collects various statistics.", mk_collect_statistics_tactic(m, p));
  ADD_TACTIC_CMD("ctx-simplify", "apply contextual simplification rules.", mk_ctx_simplify_tactic(m, p));
  ADD_TACTIC_CMD("demodulator", "extracts equalities from quantifiers and applies them to simplify.", mk_demodulator_tactic(m, p));
  ADD_TACTIC_CMD("der", "destructive equality resolution.", mk_der_tactic(m));
  ADD_TACTIC_CMD("distribute-forall", "distribute forall over conjunctions.", mk_distribute_forall_tactic(m, p));
  ADD_TACTIC_CMD("dom-simplify", "apply dominator simplification rules.", mk_dom_simplify_tactic(m, p));
  ADD_TACTIC_CMD("elim-term-ite", "eliminate term if-then-else by adding fresh auxiliary declarations.", mk_elim_term_ite_tactic(m, p));
  ADD_TACTIC_CMD("elim-uncnstr2", "eliminate unconstrained variables.", mk_elim_uncnstr2_tactic(m, p));
  ADD_TACTIC_CMD("elim-uncnstr", "eliminate application containing unconstrained variables.", mk_elim_uncnstr_tactic(m, p));
  ADD_TACTIC_CMD("elim-predicates", "eliminate predicates, macros and implicit definitions.", mk_eliminate_predicates_tactic(m, p));
  ADD_TACTIC_CMD("injectivity", "Identifies and applies injectivity axioms.", mk_injectivity_tactic(m, p));
  ADD_TACTIC_CMD("snf", "put goal in skolem normal form.", mk_snf_tactic(m, p));
  ADD_TACTIC_CMD("nnf", "put goal in negation normal form.", mk_nnf_tactic(m, p));
  ADD_TACTIC_CMD("occf", "put goal in one constraint per clause normal form (notes: fails if proof generation is enabled; only clauses are considered).", mk_occf_tactic(m, p));
  ADD_TACTIC_CMD("pb-preprocess", "pre-process pseudo-Boolean constraints a la Davis Putnam.", mk_pb_preprocess_tactic(m, p));
  ADD_TACTIC_CMD("propagate-values2", "propagate constants.", mk_propagate_values2_tactic(m, p));
  ADD_TACTIC_CMD("propagate-values", "propagate constants.", mk_propagate_values_tactic(m, p));
  ADD_TACTIC_CMD("reduce-args", "reduce the number of arguments of function applications, when for all occurrences of a function f the i-th is a value.", mk_reduce_args_tactic(m, p));
  ADD_TACTIC_CMD("reduce-args2", "reduce the number of arguments of function applications, when for all occurrences of a function f the i-th is a value.", mk_reduce_args_tactic2(m, p));
  ADD_TACTIC_CMD("simplify", "apply simplification rules.", mk_simplify_tactic(m, p));
  ADD_TACTIC_CMD("elim-and", "convert (and a b) into (not (or (not a) (not b))).", mk_elim_and_tactic(m, p));
  ADD_TACTIC_CMD("solve-eqs", "solve for variables.", mk_solve_eqs_tactic(m, p));
  ADD_TACTIC_CMD("special-relations", "detect and replace by special relations.", mk_special_relations_tactic(m, p));
  ADD_TACTIC_CMD("split-clause", "split a clause in many subgoals.", mk_split_clause_tactic(p));
  ADD_TACTIC_CMD("symmetry-reduce", "apply symmetry reduction.", mk_symmetry_reduce_tactic(m, p));
  ADD_TACTIC_CMD("tseitin-cnf", "convert goal into CNF using tseitin-like encoding (note: quantifiers are ignored).", mk_tseitin_cnf_tactic(m, p));
  ADD_TACTIC_CMD("tseitin-cnf-core", "convert goal into CNF using tseitin-like encoding (note: quantifiers are ignored). This tactic does not apply required simplifications to the input goal like the tseitin-cnf tactic.", mk_tseitin_cnf_core_tactic(m, p));
  ADD_TACTIC_CMD("qffd", "builtin strategy for solving QF_FD problems.", mk_fd_tactic(m, p));
  ADD_TACTIC_CMD("pqffd", "builtin strategy for solving QF_FD problems in parallel.", mk_parallel_qffd_tactic(m, p));
  ADD_TACTIC_CMD("smtfd", "builtin strategy for solving SMT problems by reduction to FD.", mk_smtfd_tactic(m, p));
  ADD_TACTIC_CMD("fpa2bv", "convert floating point numbers to bit-vectors.", mk_fpa2bv_tactic(m, p));
  ADD_TACTIC_CMD("qffp", "(try to) solve goal using the tactic for QF_FP.", mk_qffp_tactic(m, p));
  ADD_TACTIC_CMD("qffpbv", "(try to) solve goal using the tactic for QF_FPBV (floats+bit-vectors).", mk_qffpbv_tactic(m, p));
  ADD_TACTIC_CMD("qffplra", "(try to) solve goal using the tactic for QF_FPLRA.", mk_qffplra_tactic(m, p));
  ADD_TACTIC_CMD("default", "default strategy used when no logic is specified.", mk_default_tactic(m, p));
  ADD_TACTIC_CMD("euf-completion", "simplify using equalities.", mk_euf_completion_tactic(m, p));
  ADD_TACTIC_CMD("solver-subsumption", "remove assertions that are subsumed.", mk_solver_subsumption_tactic(m, p));
  ADD_TACTIC_CMD("qfbv-sls", "(try to) solve using stochastic local search for QF_BV.", mk_qfbv_sls_tactic(m, p));
  ADD_TACTIC_CMD("sls-smt", "(try to) solve SMT formulas using local search.", mk_sls_smt_tactic(m, p));
  ADD_TACTIC_CMD("nra", "builtin strategy for solving NRA problems.", mk_nra_tactic(m, p));
  ADD_TACTIC_CMD("qfaufbv", "builtin strategy for solving QF_AUFBV problems.", mk_qfaufbv_tactic(m, p));
  ADD_TACTIC_CMD("qfauflia", "builtin strategy for solving QF_AUFLIA problems.", mk_qfauflia_tactic(m, p));
  ADD_TACTIC_CMD("qfbv", "builtin strategy for solving QF_BV problems.", mk_qfbv_tactic(m, p));
  ADD_TACTIC_CMD("qfidl", "builtin strategy for solving QF_IDL problems.", mk_qfidl_tactic(m, p));
  ADD_TACTIC_CMD("qflia", "builtin strategy for solving QF_LIA problems.", mk_qflia_tactic(m, p));
  ADD_TACTIC_CMD("qflra", "builtin strategy for solving QF_LRA problems.", mk_qflra_tactic(m, p));
  ADD_TACTIC_CMD("qfnia", "builtin strategy for solving QF_NIA problems.", mk_qfnia_tactic(m, p));
  ADD_TACTIC_CMD("qfnra", "builtin strategy for solving QF_NRA problems.", mk_qfnra_tactic(m, p));
  ADD_TACTIC_CMD("qfuf", "builtin strategy for solving QF_UF problems.", mk_qfuf_tactic(m, p));
  ADD_TACTIC_CMD("qfufbv", "builtin strategy for solving QF_UFBV problems.", mk_qfufbv_tactic(m, p));
  ADD_TACTIC_CMD("qfufbv_ackr", "A tactic for solving QF_UFBV based on Ackermannization.", mk_qfufbv_ackr_tactic(m, p));
  ADD_TACTIC_CMD("ufnia", "builtin strategy for solving UFNIA problems.", mk_ufnia_tactic(m, p));
  ADD_TACTIC_CMD("uflra", "builtin strategy for solving UFLRA problems.", mk_uflra_tactic(m, p));
  ADD_TACTIC_CMD("auflia", "builtin strategy for solving AUFLIA problems.", mk_auflia_tactic(m, p));
  ADD_TACTIC_CMD("auflira", "builtin strategy for solving AUFLIRA problems.", mk_auflira_tactic(m, p));
  ADD_TACTIC_CMD("aufnira", "builtin strategy for solving AUFNIRA problems.", mk_aufnira_tactic(m, p));
  ADD_TACTIC_CMD("lra", "builtin strategy for solving LRA problems.", mk_lra_tactic(m, p));
  ADD_TACTIC_CMD("lia", "builtin strategy for solving LIA problems.", mk_lia_tactic(m, p));
  ADD_TACTIC_CMD("lira", "builtin strategy for solving LIRA problems.", mk_lira_tactic(m, p));
  ADD_TACTIC_CMD("smt", "apply a SAT based SMT solver.", mk_smt_tactic(m, p));
  ADD_TACTIC_CMD("skip", "do nothing tactic.", mk_skip_tactic());
  ADD_TACTIC_CMD("fail", "always fail tactic.", mk_fail_tactic());
  ADD_TACTIC_CMD("fail-if-undecided", "fail if goal is undecided.", mk_fail_if_undecided_tactic());
  ADD_TACTIC_CMD("macro-finder", "Identifies and applies macros.", mk_macro_finder_tactic(m, p));
  ADD_TACTIC_CMD("quasi-macros", "Identifies and applies quasi-macros.", mk_quasi_macros_tactic(m, p));
  ADD_TACTIC_CMD("ufbv-rewriter", "Applies UFBV-specific rewriting rules, mainly demodulation.", mk_quasi_macros_tactic(m, p));
  ADD_TACTIC_CMD("bv", "builtin strategy for solving BV problems (with quantifiers).", mk_ufbv_tactic(m, p));
  ADD_TACTIC_CMD("ufbv", "builtin strategy for solving UFBV problems (with quantifiers).", mk_ufbv_tactic(m, p));
  ADD_PROBE("ackr-bound-probe", "A probe to give an upper bound of Ackermann congruence lemmas that a formula might generate.", mk_ackr_bound_probe());
  ADD_PROBE("is-unbounded", "true if the goal contains integer/real constants that do not have lower/upper bounds.", mk_is_unbounded_probe());
  ADD_PROBE("is-pb", "true if the goal is a pseudo-boolean problem.", mk_is_pb_probe());
  ADD_PROBE("arith-max-deg", "max polynomial total degree of an arithmetic atom.", mk_arith_max_degree_probe());
  ADD_PROBE("arith-avg-deg", "avg polynomial total degree of an arithmetic atom.", mk_arith_avg_degree_probe());
  ADD_PROBE("arith-max-bw", "max coefficient bit width.", mk_arith_max_bw_probe());
  ADD_PROBE("arith-avg-bw", "avg coefficient bit width.", mk_arith_avg_bw_probe());
  ADD_PROBE("is-qflia", "true if the goal is in QF_LIA.", mk_is_qflia_probe());
  ADD_PROBE("is-qfauflia", "true if the goal is in QF_AUFLIA.", mk_is_qfauflia_probe());
  ADD_PROBE("is-qflra", "true if the goal is in QF_LRA.", mk_is_qflra_probe());
  ADD_PROBE("is-qflira", "true if the goal is in QF_LIRA.", mk_is_qflira_probe());
  ADD_PROBE("is-ilp", "true if the goal is ILP.", mk_is_ilp_probe());
  ADD_PROBE("is-qfnia", "true if the goal is in QF_NIA (quantifier-free nonlinear integer arithmetic).", mk_is_qfnia_probe());
  ADD_PROBE("is-qfnra", "true if the goal is in QF_NRA (quantifier-free nonlinear real arithmetic).", mk_is_qfnra_probe());
  ADD_PROBE("is-nia", "true if the goal is in NIA (nonlinear integer arithmetic, formula may have quantifiers).", mk_is_nia_probe());
  ADD_PROBE("is-nra", "true if the goal is in NRA (nonlinear real arithmetic, formula may have quantifiers).", mk_is_nra_probe());
  ADD_PROBE("is-nira", "true if the goal is in NIRA (nonlinear integer and real arithmetic, formula may have quantifiers).", mk_is_nira_probe());
  ADD_PROBE("is-lia", "true if the goal is in LIA (linear integer arithmetic, formula may have quantifiers).", mk_is_lia_probe());
  ADD_PROBE("is-lra", "true if the goal is in LRA (linear real arithmetic, formula may have quantifiers).", mk_is_lra_probe());
  ADD_PROBE("is-lira", "true if the goal is in LIRA (linear integer and real arithmetic, formula may have quantifiers).", mk_is_lira_probe());
  ADD_PROBE("is-qfufnra", "true if the goal is QF_UFNRA (quantifier-free nonlinear real arithmetic with other theories).", mk_is_qfufnra_probe());
  ADD_PROBE("is-qfbv-eq", "true if the goal is in a fragment of QF_BV which uses only =, extract, concat.", mk_is_qfbv_eq_probe());
  ADD_PROBE("is-qffp", "true if the goal is in QF_FP (floats).", mk_is_qffp_probe());
  ADD_PROBE("is-qffpbv", "true if the goal is in QF_FPBV (floats+bit-vectors).", mk_is_qffpbv_probe());
  ADD_PROBE("is-qffplra", "true if the goal is in QF_FPLRA.", mk_is_qffplra_probe());
  ADD_PROBE("memory", "amount of used memory in megabytes.", mk_memory_probe());
  ADD_PROBE("depth", "depth of the input goal.", mk_depth_probe());
  ADD_PROBE("size", "number of assertions in the given goal.", mk_size_probe());
  ADD_PROBE("num-exprs", "number of expressions/terms in the given goal.", mk_num_exprs_probe());
  ADD_PROBE("num-consts", "number of non Boolean constants in the given goal.", mk_num_consts_probe());
  ADD_PROBE("num-bool-consts", "number of Boolean constants in the given goal.", mk_num_bool_consts_probe());
  ADD_PROBE("num-arith-consts", "number of arithmetic constants in the given goal.", mk_num_arith_consts_probe());
  ADD_PROBE("num-bv-consts", "number of bit-vector constants in the given goal.", mk_num_bv_consts_probe());
  ADD_PROBE("produce-proofs", "true if proof generation is enabled for the given goal.", mk_produce_proofs_probe());
  ADD_PROBE("produce-model", "true if model generation is enabled for the given goal.", mk_produce_models_probe());
  ADD_PROBE("produce-unsat-cores", "true if unsat-core generation is enabled for the given goal.", mk_produce_unsat_cores_probe());
  ADD_PROBE("has-quantifiers", "true if the goal contains quantifiers.", mk_has_quantifier_probe());
  ADD_PROBE("has-patterns", "true if the goal contains quantifiers with patterns.", mk_has_pattern_probe());
  ADD_PROBE("is-propositional", "true if the goal is in propositional logic.", mk_is_propositional_probe());
  ADD_PROBE("is-qfbv", "true if the goal is in QF_BV.", mk_is_qfbv_probe());
  ADD_PROBE("is-qfaufbv", "true if the goal is in QF_AUFBV.", mk_is_qfaufbv_probe());
  ADD_PROBE("is-quasi-pb", "true if the goal is quasi-pb.", mk_is_quasi_pb_probe());
  ADD_SIMPLIFIER_CMD("bit2int", "simplify bit2int expressions.", alloc(bit2int_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("bit-blast", "reduce bit-vector expressions into SAT.", alloc(bit_blaster_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("cheap-fourier-motzkin", "eliminate variables from quantifiers using partial Fourier-Motzkin elimination.", alloc(elim_bounds_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("elim-term-ite", "eliminate if-then-else term by hoisting them top top-level.", alloc(elim_term_ite_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("pull-nested-quantifiers", "pull nested quantifiers to top-level.", alloc(pull_nested_quantifiers_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("push-app-ite-conservative", "Push functions over if-then else.", alloc(push_ite_simplifier, m, p, s, true));
  ADD_SIMPLIFIER_CMD("push-app-ite", "Push functions over if-then else.", alloc(push_ite_simplifier, m, p, s, false));
  ADD_SIMPLIFIER_CMD("ng-push-app-ite-conservative", "Push functions over if-then-else within non-ground terms only.", alloc(ng_push_ite_simplifier, m, p, s, true));
  ADD_SIMPLIFIER_CMD("ng-push-app-ite", "Push functions over if-then-else within non-ground terms only.", alloc(ng_push_ite_simplifier, m, p, s, false));
  ADD_SIMPLIFIER_CMD("randomizer", "shuffle assertions and rename uninterpreted functions.", alloc(randomizer_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("refine-injectivity", "refine injectivity axioms.", alloc(refine_inj_axiom_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("simplify", "apply simplification rules.", alloc(rewriter_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("qe-light", "apply light-weight quantifier elimination.", mk_qe_lite_simplifier(m, p, s));
  ADD_SIMPLIFIER_CMD("card2bv", "convert pseudo-boolean constraints to bit-vectors.", alloc(card2bv, m, p, s));
  ADD_SIMPLIFIER_CMD("propagate-ineqs", "propagate ineqs/bounds, remove subsumed inequalities.", alloc(bound_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("propagate-bv-bounds", "propagate bit-vector bounds by simplifying implied or contradictory bounds.", mk_bv_bounds_simplifier(m, p, s));
  ADD_SIMPLIFIER_CMD("bv-slice", "simplify using bit-vector slices.", alloc(bv::slice, m, s));
  ADD_SIMPLIFIER_CMD("demodulator", "extracts equalities from quantifiers and applies them to simplify.", alloc(demodulator_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("distribute-forall", "distribute forall over conjunctions.", alloc(distribute_forall_simplifier, m, p, s));
  ADD_SIMPLIFIER_CMD("dom-simplify", "apply dominator simplification rules.", alloc(dominator_simplifier, m, s, mk_expr_substitution_simplifier(m), p));
  ADD_SIMPLIFIER_CMD("elim-unconstrained", "eliminate unconstrained variables.", alloc(elim_unconstrained, m, s));
  ADD_SIMPLIFIER_CMD("elim-predicates", "eliminate predicates, macros and implicit definitions.", alloc(eliminate_predicates, m, s));
  ADD_SIMPLIFIER_CMD("propagate-values", "propagate constants.", alloc(propagate_values, m, p, s));
  ADD_SIMPLIFIER_CMD("reduce-args", "reduce the number of arguments of function applications, when for all occurrences of a function f the i-th is a value.", mk_reduce_args_simplifier(m, s, p));
  ADD_SIMPLIFIER_CMD("solve-eqs", "solve for variables.", alloc(euf::solve_eqs, m, s));
  ADD_SIMPLIFIER_CMD("euf-completion", "simplify modulo congruence closure.", mk_euf_completion_simplifier(m, s, p));
}
