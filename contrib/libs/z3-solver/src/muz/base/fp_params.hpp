// Automatically generated file
#pragma once
#include "util/params.h"
#include "util/gparams.h"
struct fp_params {
  params_ref const & p;
  params_ref g;
  fp_params(params_ref const & _p = params_ref::get_empty()):
     p(_p), g(gparams::get_module("fp")) {}
  static void collect_param_descrs(param_descrs & d) {
    d.insert("engine", CPK_SYMBOL, "Select: auto-config, datalog, bmc, spacer", "auto-config","fp");
    d.insert("datalog.default_table", CPK_SYMBOL, "default table implementation: sparse, hashtable, bitvector, interval", "sparse","fp");
    d.insert("datalog.default_relation", CPK_SYMBOL, "default relation implementation: external_relation, pentagon", "pentagon","fp");
    d.insert("datalog.generate_explanations", CPK_BOOL, "produce explanations for produced facts when using the datalog engine", "false","fp");
    d.insert("datalog.use_map_names", CPK_BOOL, "use names from map files when displaying tuples", "true","fp");
    d.insert("datalog.magic_sets_for_queries", CPK_BOOL, "magic set transformation will be used for queries", "false","fp");
    d.insert("datalog.explanations_on_relation_level", CPK_BOOL, "if true, explanations are generated as history of each relation, rather than per fact (generate_explanations must be set to true for this option to have any effect)", "false","fp");
    d.insert("datalog.unbound_compressor", CPK_BOOL, "auxiliary relations will be introduced to avoid unbound variables in rule heads", "true","fp");
    d.insert("datalog.similarity_compressor", CPK_BOOL, "rules that differ only in values of constants will be merged into a single rule", "true","fp");
    d.insert("datalog.similarity_compressor_threshold", CPK_UINT, "if similarity_compressor is on, this value determines how many similar rules there must be in order for them to be merged", "11","fp");
    d.insert("datalog.all_or_nothing_deltas", CPK_BOOL, "compile rules so that it is enough for the delta relation in union and widening operations to determine only whether the updated relation was modified or not", "false","fp");
    d.insert("datalog.compile_with_widening", CPK_BOOL, "widening will be used to compile recursive rules", "false","fp");
    d.insert("datalog.default_table_checked", CPK_BOOL, "if true, the default table will be default_table inside a wrapper that checks that its results are the same as of default_table_checker table", "false","fp");
    d.insert("datalog.default_table_checker", CPK_SYMBOL, "see default_table_checked", "null","fp");
    d.insert("datalog.check_relation", CPK_SYMBOL, "name of default relation to check. operations on the default relation will be verified using SMT solving", "null","fp");
    d.insert("datalog.initial_restart_timeout", CPK_UINT, "length of saturation run before the first restart (in ms), zero means no restarts", "0","fp");
    d.insert("datalog.timeout", CPK_UINT, "Time limit used for saturation", "0","fp");
    d.insert("datalog.output_profile", CPK_BOOL, "determines whether profile information should be output when outputting Datalog rules or instructions", "false","fp");
    d.insert("datalog.print.tuples", CPK_BOOL, "determines whether tuples for output predicates should be output", "true","fp");
    d.insert("datalog.profile_timeout_milliseconds", CPK_UINT, "instructions and rules that took less than the threshold will not be printed when printed the instruction/rule list", "0","fp");
    d.insert("datalog.dbg_fpr_nonempty_relation_signature", CPK_BOOL, "if true, finite_product_relation will attempt to avoid creating inner relation with empty signature by putting in half of the table columns, if it would have been empty otherwise", "false","fp");
    d.insert("datalog.subsumption", CPK_BOOL, "if true, removes/filters predicates with total transitions", "true","fp");
    d.insert("generate_proof_trace", CPK_BOOL, "trace for 'sat' answer as proof object", "false","fp");
    d.insert("spacer.push_pob", CPK_BOOL, "push blocked pobs to higher level", "false","fp");
    d.insert("spacer.push_pob_max_depth", CPK_UINT, "Maximum depth at which push_pob is enabled", "4294967295","fp");
    d.insert("validate", CPK_BOOL, "validate result (by proof checking or model checking)", "false","fp");
    d.insert("spacer.simplify_lemmas_pre", CPK_BOOL, "simplify derived lemmas before inductive propagation", "false","fp");
    d.insert("spacer.simplify_lemmas_post", CPK_BOOL, "simplify derived lemmas after inductive propagation", "false","fp");
    d.insert("spacer.use_inductive_generalizer", CPK_BOOL, "generalize lemmas using induction strengthening", "true","fp");
    d.insert("spacer.max_num_contexts", CPK_UINT, "maximal number of contexts to create", "500","fp");
    d.insert("print_fixedpoint_extensions", CPK_BOOL, "use SMT-LIB2 fixedpoint extensions, instead of pure SMT2, when printing rules", "true","fp");
    d.insert("print_low_level_smt2", CPK_BOOL, "use (faster) low-level SMT2 printer (the printer is scalable but the result may not be as readable)", "false","fp");
    d.insert("print_with_variable_declarations", CPK_BOOL, "use variable declarations when displaying rules (instead of attempting to use original names)", "true","fp");
    d.insert("print_answer", CPK_BOOL, "print answer instance(s) to query", "false","fp");
    d.insert("print_certificate", CPK_BOOL, "print certificate for reachability or non-reachability", "false","fp");
    d.insert("print_boogie_certificate", CPK_BOOL, "print certificate for reachability or non-reachability using a format understood by Boogie", "false","fp");
    d.insert("print_aig", CPK_SYMBOL, "Dump clauses in AIG text format (AAG) to the given file name", "","fp");
    d.insert("print_statistics", CPK_BOOL, "print statistics", "false","fp");
    d.insert("tab.selection", CPK_SYMBOL, "selection method for tabular strategy: weight (default), first, var-use", "weight","fp");
    d.insert("xform.bit_blast", CPK_BOOL, "bit-blast bit-vectors", "false","fp");
    d.insert("xform.magic", CPK_BOOL, "perform symbolic magic set transformation", "false","fp");
    d.insert("xform.scale", CPK_BOOL, "add scaling variable to linear real arithmetic clauses", "false","fp");
    d.insert("xform.inline_linear", CPK_BOOL, "try linear inlining method", "true","fp");
    d.insert("xform.inline_eager", CPK_BOOL, "try eager inlining of rules", "true","fp");
    d.insert("xform.inline_linear_branch", CPK_BOOL, "try linear inlining method with potential expansion", "false","fp");
    d.insert("xform.compress_unbound", CPK_BOOL, "compress tails with unbound variables", "true","fp");
    d.insert("xform.fix_unbound_vars", CPK_BOOL, "fix unbound variables in tail", "false","fp");
    d.insert("xform.unfold_rules", CPK_UINT, "unfold rules statically using iterative squaring", "0","fp");
    d.insert("xform.slice", CPK_BOOL, "simplify clause set using slicing", "true","fp");
    d.insert("spacer.use_euf_gen", CPK_BOOL, "Generalize lemmas and pobs using implied equalities", "false","fp");
    d.insert("xform.transform_arrays", CPK_BOOL, "Rewrites arrays equalities and applies select over store", "false","fp");
    d.insert("xform.instantiate_arrays", CPK_BOOL, "Transforms P(a) into P(i, a[i] a)", "false","fp");
    d.insert("xform.instantiate_arrays.enforce", CPK_BOOL, "Transforms P(a) into P(i, a[i]), discards a from predicate", "false","fp");
    d.insert("xform.instantiate_arrays.nb_quantifier", CPK_UINT, "Gives the number of quantifiers per array", "1","fp");
    d.insert("xform.instantiate_arrays.slice_technique", CPK_SYMBOL, "<no-slicing>=> GetId(i) = i, <smash> => GetId(i) = true", "no-slicing","fp");
    d.insert("xform.quantify_arrays", CPK_BOOL, "create quantified Horn clauses from clauses with arrays", "false","fp");
    d.insert("xform.instantiate_quantifiers", CPK_BOOL, "instantiate quantified Horn clauses using E-matching heuristic", "false","fp");
    d.insert("xform.coalesce_rules", CPK_BOOL, "coalesce rules", "false","fp");
    d.insert("xform.tail_simplifier_pve", CPK_BOOL, "propagate_variable_equivalences", "true","fp");
    d.insert("xform.subsumption_checker", CPK_BOOL, "Enable subsumption checker (no support for model conversion)", "true","fp");
    d.insert("xform.coi", CPK_BOOL, "use cone of influence simplification", "true","fp");
    d.insert("spacer.order_children", CPK_UINT, "SPACER: order of enqueuing children in non-linear rules : 0 (original), 1 (reverse), 2 (random)", "0","fp");
    d.insert("spacer.use_lemma_as_cti", CPK_BOOL, "SPACER: use a lemma instead of a CTI in flexible_trace", "false","fp");
    d.insert("spacer.reset_pob_queue", CPK_BOOL, "SPACER: reset pob obligation queue when entering a new level", "true","fp");
    d.insert("spacer.use_array_eq_generalizer", CPK_BOOL, "SPACER: attempt to generalize lemmas with array equalities", "true","fp");
    d.insert("spacer.use_derivations", CPK_BOOL, "SPACER: using derivation mechanism to cache intermediate results for non-linear rules", "true","fp");
    d.insert("xform.array_blast", CPK_BOOL, "try to eliminate local array terms using Ackermannization -- some array terms may remain", "false","fp");
    d.insert("xform.array_blast_full", CPK_BOOL, "eliminate all local array variables by QE", "false","fp");
    d.insert("xform.elim_term_ite", CPK_BOOL, "Eliminate term-ite expressions", "false","fp");
    d.insert("xform.elim_term_ite.inflation", CPK_UINT, "Maximum inflation for non-Boolean ite-terms blasting: 0 (none), k (multiplicative)", "3","fp");
    d.insert("spacer.propagate", CPK_BOOL, "Enable propagate/pushing phase", "true","fp");
    d.insert("spacer.max_level", CPK_UINT, "Maximum level to explore", "4294967295","fp");
    d.insert("spacer.elim_aux", CPK_BOOL, "Eliminate auxiliary variables in reachability facts", "true","fp");
    d.insert("spacer.blast_term_ite_inflation", CPK_UINT, "Maximum inflation for non-Boolean ite-terms expansion: 0 (none), k (multiplicative)", "3","fp");
    d.insert("spacer.reach_dnf", CPK_BOOL, "Restrict reachability facts to DNF", "true","fp");
    d.insert("bmc.linear_unrolling_depth", CPK_UINT, "Maximal level to explore", "4294967295","fp");
    d.insert("spacer.iuc.split_farkas_literals", CPK_BOOL, "Split Farkas literals", "false","fp");
    d.insert("spacer.native_mbp", CPK_BOOL, "Use native mbp of Z3", "true","fp");
    d.insert("spacer.eq_prop", CPK_BOOL, "Enable equality and bound propagation in arithmetic", "true","fp");
    d.insert("spacer.weak_abs", CPK_BOOL, "Weak abstraction", "true","fp");
    d.insert("spacer.restarts", CPK_BOOL, "Enable resetting obligation queue", "false","fp");
    d.insert("spacer.restart_initial_threshold", CPK_UINT, "Initial threshold for restarts", "10","fp");
    d.insert("spacer.random_seed", CPK_UINT, "Random seed to be used by SMT solver", "0","fp");
    d.insert("spacer.mbqi", CPK_BOOL, "Enable mbqi", "true","fp");
    d.insert("spacer.keep_proxy", CPK_BOOL, "keep proxy variables (internal parameter)", "true","fp");
    d.insert("spacer.q3", CPK_BOOL, "Allow quantified lemmas in frames", "true","fp");
    d.insert("spacer.q3.instantiate", CPK_BOOL, "Instantiate quantified lemmas", "true","fp");
    d.insert("spacer.q3.use_qgen", CPK_BOOL, "use quantified lemma generalizer", "false","fp");
    d.insert("spacer.q3.qgen.normalize", CPK_BOOL, "normalize cube before quantified generalization", "true","fp");
    d.insert("spacer.iuc", CPK_UINT, "0 = use old implementation of unsat-core-generation, 1 = use new implementation of IUC generation, 2 = use new implementation of IUC + min-cut optimization", "1","fp");
    d.insert("spacer.iuc.arith", CPK_UINT, "0 = use simple Farkas plugin, 1 = use simple Farkas plugin with constant from other partition (like old unsat-core-generation),2 = use Gaussian elimination optimization (broken), 3 = use additive IUC plugin", "1","fp");
    d.insert("spacer.iuc.old_hyp_reducer", CPK_BOOL, "use old hyp reducer instead of new implementation, for debugging only", "false","fp");
    d.insert("spacer.validate_lemmas", CPK_BOOL, "Validate each lemma after generalization", "false","fp");
    d.insert("spacer.ground_pobs", CPK_BOOL, "Ground pobs by using values from a model", "true","fp");
    d.insert("spacer.iuc.print_farkas_stats", CPK_BOOL, "prints for each proof how many Farkas lemmas it contains and how many of these participate in the cut (for debugging)", "false","fp");
    d.insert("spacer.iuc.debug_proof", CPK_BOOL, "prints proof used by unsat-core-learner for debugging purposes (debugging)", "false","fp");
    d.insert("spacer.simplify_pob", CPK_BOOL, "simplify pobs by removing redundant constraints", "false","fp");
    d.insert("spacer.p3.share_lemmas", CPK_BOOL, "Share frame lemmas", "false","fp");
    d.insert("spacer.p3.share_invariants", CPK_BOOL, "Share invariants lemmas", "false","fp");
    d.insert("spacer.min_level", CPK_UINT, "Minimal level to explore", "0","fp");
    d.insert("spacer.trace_file", CPK_SYMBOL, "Log file for progress events", "","fp");
    d.insert("spacer.ctp", CPK_BOOL, "Enable counterexample-to-pushing", "true","fp");
    d.insert("spacer.use_inc_clause", CPK_BOOL, "Use incremental clause to represent trans", "true","fp");
    d.insert("spacer.dump_benchmarks", CPK_BOOL, "Dump SMT queries as benchmarks", "false","fp");
    d.insert("spacer.dump_threshold", CPK_DOUBLE, "Threshold in seconds on dumping benchmarks", "5.0","fp");
    d.insert("spacer.gpdr", CPK_BOOL, "Use GPDR solving strategy for non-linear CHC", "false","fp");
    d.insert("spacer.gpdr.bfs", CPK_BOOL, "Use BFS exploration strategy for expanding model search", "true","fp");
    d.insert("spacer.use_bg_invs", CPK_BOOL, "Enable external background invariants", "false","fp");
    d.insert("spacer.use_lim_num_gen", CPK_BOOL, "Enable limit numbers generalizer to get smaller numbers", "false","fp");
    d.insert("spacer.logic", CPK_SYMBOL, "SMT-LIB logic to configure internal SMT solvers", "","fp");
    d.insert("spacer.arith.solver", CPK_UINT, "arithmetic solver: 0 - no solver, 1 - bellman-ford based solver (diff. logic only), 2 - simplex based solver, 3 - floyd-warshall based solver (diff. logic only) and no theory combination 4 - utvpi, 5 - infinitary lra, 6 - lra solver", "2","fp");
    d.insert("spacer.global", CPK_BOOL, "Enable global guidance", "false","fp");
    d.insert("spacer.gg.concretize", CPK_BOOL, "Enable global guidance concretize", "true","fp");
    d.insert("spacer.gg.conjecture", CPK_BOOL, "Enable global guidance conjecture", "true","fp");
    d.insert("spacer.gg.subsume", CPK_BOOL, "Enable global guidance subsume", "true","fp");
    d.insert("spacer.use_iuc", CPK_BOOL, "Enable Interpolating Unsat Core(IUC) for lemma generalization", "true","fp");
    d.insert("spacer.expand_bnd", CPK_BOOL, "Enable expand-bound lemma generalization", "false","fp");
  }
  /*
     REG_MODULE_PARAMS('fp', 'fp_params::collect_param_descrs')
     REG_MODULE_DESCRIPTION('fp', 'fixedpoint parameters')
  */
  symbol engine() const { return p.get_sym("engine", g, symbol("auto-config")); }
  symbol datalog_default_table() const { return p.get_sym("datalog.default_table", g, symbol("sparse")); }
  symbol datalog_default_relation() const { return p.get_sym("datalog.default_relation", g, symbol("pentagon")); }
  bool datalog_generate_explanations() const { return p.get_bool("datalog.generate_explanations", g, false); }
  bool datalog_use_map_names() const { return p.get_bool("datalog.use_map_names", g, true); }
  bool datalog_magic_sets_for_queries() const { return p.get_bool("datalog.magic_sets_for_queries", g, false); }
  bool datalog_explanations_on_relation_level() const { return p.get_bool("datalog.explanations_on_relation_level", g, false); }
  bool datalog_unbound_compressor() const { return p.get_bool("datalog.unbound_compressor", g, true); }
  bool datalog_similarity_compressor() const { return p.get_bool("datalog.similarity_compressor", g, true); }
  unsigned datalog_similarity_compressor_threshold() const { return p.get_uint("datalog.similarity_compressor_threshold", g, 11u); }
  bool datalog_all_or_nothing_deltas() const { return p.get_bool("datalog.all_or_nothing_deltas", g, false); }
  bool datalog_compile_with_widening() const { return p.get_bool("datalog.compile_with_widening", g, false); }
  bool datalog_default_table_checked() const { return p.get_bool("datalog.default_table_checked", g, false); }
  symbol datalog_default_table_checker() const { return p.get_sym("datalog.default_table_checker", g, symbol("null")); }
  symbol datalog_check_relation() const { return p.get_sym("datalog.check_relation", g, symbol("null")); }
  unsigned datalog_initial_restart_timeout() const { return p.get_uint("datalog.initial_restart_timeout", g, 0u); }
  unsigned datalog_timeout() const { return p.get_uint("datalog.timeout", g, 0u); }
  bool datalog_output_profile() const { return p.get_bool("datalog.output_profile", g, false); }
  bool datalog_print_tuples() const { return p.get_bool("datalog.print.tuples", g, true); }
  unsigned datalog_profile_timeout_milliseconds() const { return p.get_uint("datalog.profile_timeout_milliseconds", g, 0u); }
  bool datalog_dbg_fpr_nonempty_relation_signature() const { return p.get_bool("datalog.dbg_fpr_nonempty_relation_signature", g, false); }
  bool datalog_subsumption() const { return p.get_bool("datalog.subsumption", g, true); }
  bool generate_proof_trace() const { return p.get_bool("generate_proof_trace", g, false); }
  bool spacer_push_pob() const { return p.get_bool("spacer.push_pob", g, false); }
  unsigned spacer_push_pob_max_depth() const { return p.get_uint("spacer.push_pob_max_depth", g, 4294967295u); }
  bool validate() const { return p.get_bool("validate", g, false); }
  bool spacer_simplify_lemmas_pre() const { return p.get_bool("spacer.simplify_lemmas_pre", g, false); }
  bool spacer_simplify_lemmas_post() const { return p.get_bool("spacer.simplify_lemmas_post", g, false); }
  bool spacer_use_inductive_generalizer() const { return p.get_bool("spacer.use_inductive_generalizer", g, true); }
  unsigned spacer_max_num_contexts() const { return p.get_uint("spacer.max_num_contexts", g, 500u); }
  bool print_fixedpoint_extensions() const { return p.get_bool("print_fixedpoint_extensions", g, true); }
  bool print_low_level_smt2() const { return p.get_bool("print_low_level_smt2", g, false); }
  bool print_with_variable_declarations() const { return p.get_bool("print_with_variable_declarations", g, true); }
  bool print_answer() const { return p.get_bool("print_answer", g, false); }
  bool print_certificate() const { return p.get_bool("print_certificate", g, false); }
  bool print_boogie_certificate() const { return p.get_bool("print_boogie_certificate", g, false); }
  symbol print_aig() const { return p.get_sym("print_aig", g, symbol("")); }
  bool print_statistics() const { return p.get_bool("print_statistics", g, false); }
  symbol tab_selection() const { return p.get_sym("tab.selection", g, symbol("weight")); }
  bool xform_bit_blast() const { return p.get_bool("xform.bit_blast", g, false); }
  bool xform_magic() const { return p.get_bool("xform.magic", g, false); }
  bool xform_scale() const { return p.get_bool("xform.scale", g, false); }
  bool xform_inline_linear() const { return p.get_bool("xform.inline_linear", g, true); }
  bool xform_inline_eager() const { return p.get_bool("xform.inline_eager", g, true); }
  bool xform_inline_linear_branch() const { return p.get_bool("xform.inline_linear_branch", g, false); }
  bool xform_compress_unbound() const { return p.get_bool("xform.compress_unbound", g, true); }
  bool xform_fix_unbound_vars() const { return p.get_bool("xform.fix_unbound_vars", g, false); }
  unsigned xform_unfold_rules() const { return p.get_uint("xform.unfold_rules", g, 0u); }
  bool xform_slice() const { return p.get_bool("xform.slice", g, true); }
  bool spacer_use_euf_gen() const { return p.get_bool("spacer.use_euf_gen", g, false); }
  bool xform_transform_arrays() const { return p.get_bool("xform.transform_arrays", g, false); }
  bool xform_instantiate_arrays() const { return p.get_bool("xform.instantiate_arrays", g, false); }
  bool xform_instantiate_arrays_enforce() const { return p.get_bool("xform.instantiate_arrays.enforce", g, false); }
  unsigned xform_instantiate_arrays_nb_quantifier() const { return p.get_uint("xform.instantiate_arrays.nb_quantifier", g, 1u); }
  symbol xform_instantiate_arrays_slice_technique() const { return p.get_sym("xform.instantiate_arrays.slice_technique", g, symbol("no-slicing")); }
  bool xform_quantify_arrays() const { return p.get_bool("xform.quantify_arrays", g, false); }
  bool xform_instantiate_quantifiers() const { return p.get_bool("xform.instantiate_quantifiers", g, false); }
  bool xform_coalesce_rules() const { return p.get_bool("xform.coalesce_rules", g, false); }
  bool xform_tail_simplifier_pve() const { return p.get_bool("xform.tail_simplifier_pve", g, true); }
  bool xform_subsumption_checker() const { return p.get_bool("xform.subsumption_checker", g, true); }
  bool xform_coi() const { return p.get_bool("xform.coi", g, true); }
  unsigned spacer_order_children() const { return p.get_uint("spacer.order_children", g, 0u); }
  bool spacer_use_lemma_as_cti() const { return p.get_bool("spacer.use_lemma_as_cti", g, false); }
  bool spacer_reset_pob_queue() const { return p.get_bool("spacer.reset_pob_queue", g, true); }
  bool spacer_use_array_eq_generalizer() const { return p.get_bool("spacer.use_array_eq_generalizer", g, true); }
  bool spacer_use_derivations() const { return p.get_bool("spacer.use_derivations", g, true); }
  bool xform_array_blast() const { return p.get_bool("xform.array_blast", g, false); }
  bool xform_array_blast_full() const { return p.get_bool("xform.array_blast_full", g, false); }
  bool xform_elim_term_ite() const { return p.get_bool("xform.elim_term_ite", g, false); }
  unsigned xform_elim_term_ite_inflation() const { return p.get_uint("xform.elim_term_ite.inflation", g, 3u); }
  bool spacer_propagate() const { return p.get_bool("spacer.propagate", g, true); }
  unsigned spacer_max_level() const { return p.get_uint("spacer.max_level", g, 4294967295u); }
  bool spacer_elim_aux() const { return p.get_bool("spacer.elim_aux", g, true); }
  unsigned spacer_blast_term_ite_inflation() const { return p.get_uint("spacer.blast_term_ite_inflation", g, 3u); }
  bool spacer_reach_dnf() const { return p.get_bool("spacer.reach_dnf", g, true); }
  unsigned bmc_linear_unrolling_depth() const { return p.get_uint("bmc.linear_unrolling_depth", g, 4294967295u); }
  bool spacer_iuc_split_farkas_literals() const { return p.get_bool("spacer.iuc.split_farkas_literals", g, false); }
  bool spacer_native_mbp() const { return p.get_bool("spacer.native_mbp", g, true); }
  bool spacer_eq_prop() const { return p.get_bool("spacer.eq_prop", g, true); }
  bool spacer_weak_abs() const { return p.get_bool("spacer.weak_abs", g, true); }
  bool spacer_restarts() const { return p.get_bool("spacer.restarts", g, false); }
  unsigned spacer_restart_initial_threshold() const { return p.get_uint("spacer.restart_initial_threshold", g, 10u); }
  unsigned spacer_random_seed() const { return p.get_uint("spacer.random_seed", g, 0u); }
  bool spacer_mbqi() const { return p.get_bool("spacer.mbqi", g, true); }
  bool spacer_keep_proxy() const { return p.get_bool("spacer.keep_proxy", g, true); }
  bool spacer_q3() const { return p.get_bool("spacer.q3", g, true); }
  bool spacer_q3_instantiate() const { return p.get_bool("spacer.q3.instantiate", g, true); }
  bool spacer_q3_use_qgen() const { return p.get_bool("spacer.q3.use_qgen", g, false); }
  bool spacer_q3_qgen_normalize() const { return p.get_bool("spacer.q3.qgen.normalize", g, true); }
  unsigned spacer_iuc() const { return p.get_uint("spacer.iuc", g, 1u); }
  unsigned spacer_iuc_arith() const { return p.get_uint("spacer.iuc.arith", g, 1u); }
  bool spacer_iuc_old_hyp_reducer() const { return p.get_bool("spacer.iuc.old_hyp_reducer", g, false); }
  bool spacer_validate_lemmas() const { return p.get_bool("spacer.validate_lemmas", g, false); }
  bool spacer_ground_pobs() const { return p.get_bool("spacer.ground_pobs", g, true); }
  bool spacer_iuc_print_farkas_stats() const { return p.get_bool("spacer.iuc.print_farkas_stats", g, false); }
  bool spacer_iuc_debug_proof() const { return p.get_bool("spacer.iuc.debug_proof", g, false); }
  bool spacer_simplify_pob() const { return p.get_bool("spacer.simplify_pob", g, false); }
  bool spacer_p3_share_lemmas() const { return p.get_bool("spacer.p3.share_lemmas", g, false); }
  bool spacer_p3_share_invariants() const { return p.get_bool("spacer.p3.share_invariants", g, false); }
  unsigned spacer_min_level() const { return p.get_uint("spacer.min_level", g, 0u); }
  symbol spacer_trace_file() const { return p.get_sym("spacer.trace_file", g, symbol("")); }
  bool spacer_ctp() const { return p.get_bool("spacer.ctp", g, true); }
  bool spacer_use_inc_clause() const { return p.get_bool("spacer.use_inc_clause", g, true); }
  bool spacer_dump_benchmarks() const { return p.get_bool("spacer.dump_benchmarks", g, false); }
  double spacer_dump_threshold() const { return p.get_double("spacer.dump_threshold", g, 5.0); }
  bool spacer_gpdr() const { return p.get_bool("spacer.gpdr", g, false); }
  bool spacer_gpdr_bfs() const { return p.get_bool("spacer.gpdr.bfs", g, true); }
  bool spacer_use_bg_invs() const { return p.get_bool("spacer.use_bg_invs", g, false); }
  bool spacer_use_lim_num_gen() const { return p.get_bool("spacer.use_lim_num_gen", g, false); }
  symbol spacer_logic() const { return p.get_sym("spacer.logic", g, symbol("")); }
  unsigned spacer_arith_solver() const { return p.get_uint("spacer.arith.solver", g, 2u); }
  bool spacer_global() const { return p.get_bool("spacer.global", g, false); }
  bool spacer_gg_concretize() const { return p.get_bool("spacer.gg.concretize", g, true); }
  bool spacer_gg_conjecture() const { return p.get_bool("spacer.gg.conjecture", g, true); }
  bool spacer_gg_subsume() const { return p.get_bool("spacer.gg.subsume", g, true); }
  bool spacer_use_iuc() const { return p.get_bool("spacer.use_iuc", g, true); }
  bool spacer_expand_bnd() const { return p.get_bool("spacer.expand_bnd", g, false); }
};
