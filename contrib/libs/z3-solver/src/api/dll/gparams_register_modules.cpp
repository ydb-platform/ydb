// Automatically generated file.
#include "util/gparams.h"
#include "ackermannization/ackermannization_params.hpp"
#include "ackermannization/ackermannize_bv_tactic_params.hpp"
#include "ast/normal_forms/nnf.h"
#include "ast/normal_forms/nnf_params.hpp"
#include "ast/pp_params.hpp"
#include "math/lp/lp_params_helper.hpp"
#include "math/polynomial/algebraic_params.hpp"
#include "math/realclosure/rcf_params.hpp"
#include "model/model_evaluator_params.hpp"
#include "model/model_params.hpp"
#include "muz/base/fp_params.hpp"
#include "nlsat/nlsat_params.hpp"
#include "opt/opt_params.hpp"
#include "params/arith_rewriter_params.hpp"
#include "params/array_rewriter_params.hpp"
#include "params/bool_rewriter_params.hpp"
#include "params/bv_rewriter_params.hpp"
#include "params/context_params.h"
#include "params/fpa2bv_rewriter_params.hpp"
#include "params/fpa_rewriter_params.hpp"
#include "params/pattern_inference_params_helper.hpp"
#include "params/poly_rewriter_params.hpp"
#include "params/rewriter_params.hpp"
#include "params/sat_params.hpp"
#include "params/seq_rewriter_params.hpp"
#include "params/sls_params.hpp"
#include "params/smt_params_helper.hpp"
#include "params/solver_params.hpp"
#include "params/tactic_params.hpp"
#include "parsers/util/parser_params.hpp"
#include "sat/sat_asymm_branch_params.hpp"
#include "sat/sat_scc_params.hpp"
#include "sat/sat_simplifier_params.hpp"
#include "solver/combined_solver_params.hpp"
#include "solver/parallel_params.hpp"
#include "tactic/smtlogics/qfufbv_tactic_params.hpp"
#include "util/env_params.h"
void gparams_register_modules() {
{ param_descrs d; context_params::collect_param_descrs(d); gparams::register_global(d); }
{ param_descrs d; env_params::collect_param_descrs(d); gparams::register_global(d); }
{ auto f = []() { auto* d = alloc(param_descrs); ackermannization_params::collect_param_descrs(*d); return d; }; gparams::register_module("ackermannization", f); }
{ auto f = []() { auto* d = alloc(param_descrs); ackermannize_bv_tactic_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); nnf::get_param_descrs(*d); return d; }; gparams::register_module("nnf", f); }
{ auto f = []() { auto* d = alloc(param_descrs); nnf_params::collect_param_descrs(*d); return d; }; gparams::register_module("nnf", f); }
{ auto f = []() { auto* d = alloc(param_descrs); pp_params::collect_param_descrs(*d); return d; }; gparams::register_module("pp", f); }
{ auto f = []() { auto* d = alloc(param_descrs); lp_params_helper::collect_param_descrs(*d); return d; }; gparams::register_module("lp", f); }
{ auto f = []() { auto* d = alloc(param_descrs); algebraic_params::collect_param_descrs(*d); return d; }; gparams::register_module("algebraic", f); }
{ auto f = []() { auto* d = alloc(param_descrs); rcf_params::collect_param_descrs(*d); return d; }; gparams::register_module("rcf", f); }
{ auto f = []() { auto* d = alloc(param_descrs); model_evaluator_params::collect_param_descrs(*d); return d; }; gparams::register_module("model_evaluator", f); }
{ auto f = []() { auto* d = alloc(param_descrs); model_params::collect_param_descrs(*d); return d; }; gparams::register_module("model", f); }
{ auto f = []() { auto* d = alloc(param_descrs); fp_params::collect_param_descrs(*d); return d; }; gparams::register_module("fp", f); }
{ auto f = []() { auto* d = alloc(param_descrs); nlsat_params::collect_param_descrs(*d); return d; }; gparams::register_module("nlsat", f); }
{ auto f = []() { auto* d = alloc(param_descrs); opt_params::collect_param_descrs(*d); return d; }; gparams::register_module("opt", f); }
{ auto f = []() { auto* d = alloc(param_descrs); arith_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); array_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); bool_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); bv_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); fpa2bv_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); fpa_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); pattern_inference_params_helper::collect_param_descrs(*d); return d; }; gparams::register_module("pi", f); }
{ auto f = []() { auto* d = alloc(param_descrs); poly_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); sat_params::collect_param_descrs(*d); return d; }; gparams::register_module("sat", f); }
{ auto f = []() { auto* d = alloc(param_descrs); seq_rewriter_params::collect_param_descrs(*d); return d; }; gparams::register_module("rewriter", f); }
{ auto f = []() { auto* d = alloc(param_descrs); sls_params::collect_param_descrs(*d); return d; }; gparams::register_module("sls", f); }
{ auto f = []() { auto* d = alloc(param_descrs); smt_params_helper::collect_param_descrs(*d); return d; }; gparams::register_module("smt", f); }
{ auto f = []() { auto* d = alloc(param_descrs); solver_params::collect_param_descrs(*d); return d; }; gparams::register_module("solver", f); }
{ auto f = []() { auto* d = alloc(param_descrs); tactic_params::collect_param_descrs(*d); return d; }; gparams::register_module("tactic", f); }
{ auto f = []() { auto* d = alloc(param_descrs); parser_params::collect_param_descrs(*d); return d; }; gparams::register_module("parser", f); }
{ auto f = []() { auto* d = alloc(param_descrs); sat_asymm_branch_params::collect_param_descrs(*d); return d; }; gparams::register_module("sat", f); }
{ auto f = []() { auto* d = alloc(param_descrs); sat_scc_params::collect_param_descrs(*d); return d; }; gparams::register_module("sat", f); }
{ auto f = []() { auto* d = alloc(param_descrs); sat_simplifier_params::collect_param_descrs(*d); return d; }; gparams::register_module("sat", f); }
{ auto f = []() { auto* d = alloc(param_descrs); combined_solver_params::collect_param_descrs(*d); return d; }; gparams::register_module("combined_solver", f); }
{ auto f = []() { auto* d = alloc(param_descrs); parallel_params::collect_param_descrs(*d); return d; }; gparams::register_module("parallel", f); }
{ auto f = []() { auto* d = alloc(param_descrs); qfufbv_tactic_params::collect_param_descrs(*d); return d; }; gparams::register_module("ackermannization", f); }
gparams::register_module_descr("ackermannization", "solving UF via ackermannization");
gparams::register_module_descr("nnf", "negation normal form");
gparams::register_module_descr("pp", "pretty printer");
gparams::register_module_descr("lp", "linear programming parameters");
gparams::register_module_descr("algebraic", "real algebraic number package. Non-default parameter settings are not supported");
gparams::register_module_descr("rcf", "real closed fields");
gparams::register_module_descr("fp", "fixedpoint parameters");
gparams::register_module_descr("nlsat", "nonlinear solver");
gparams::register_module_descr("opt", "optimization parameters");
gparams::register_module_descr("pi", "pattern inference (heuristics) for universal formulas (without annotation)");
gparams::register_module_descr("rewriter", "new formula simplification module used in the tactic framework, and new solvers");
gparams::register_module_descr("sat", "propositional SAT solver");
gparams::register_module_descr("sls", "Stochastic Local Search Solver (invoked by sls-qfbv and sls-smt tactics or enabled by smt.sls.enable=true)");
gparams::register_module_descr("smt", "smt solver based on lazy smt");
gparams::register_module_descr("solver", "solver parameters");
gparams::register_module_descr("tactic", "tactic parameters");
gparams::register_module_descr("combined_solver", "combines two solvers: non-incremental (solver1) and incremental (solver2)");
gparams::register_module_descr("parallel", "parameters for parallel solver");
gparams::register_module_descr("ackermannization", "tactics based on solving UF-theories via ackermannization (see also ackr module)");
}
