/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2022 at the University of Edinburgh    */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/*    Authors: Julian Hall, Ivet Galabova, Leona Gottwald and Michael    */
/*    Feldmeier                                                          */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/**@file lp_data/HSimplexDebug.cpp
 * @brief
 */

#include "simplex/HSimplexReport.h"

#include <sstream>

void reportSimplexPhaseIterations(const HighsLogOptions& log_options,
                                  const HighsInt iteration_count,
                                  const HighsSimplexInfo& info,
                                  const bool initialise) {
  if (info.run_quiet) return;
  static HighsInt iteration_count0 = 0;
  static HighsInt dual_phase1_iteration_count0 = 0;
  static HighsInt dual_phase2_iteration_count0 = 0;
  static HighsInt primal_phase1_iteration_count0 = 0;
  static HighsInt primal_phase2_iteration_count0 = 0;
  static HighsInt primal_bound_swap0 = 0;
  if (initialise) {
    iteration_count0 = iteration_count;
    dual_phase1_iteration_count0 = info.dual_phase1_iteration_count;
    dual_phase2_iteration_count0 = info.dual_phase2_iteration_count;
    primal_phase1_iteration_count0 = info.primal_phase1_iteration_count;
    primal_phase2_iteration_count0 = info.primal_phase2_iteration_count;
    primal_bound_swap0 = info.primal_bound_swap;
    return;
  }
  const HighsInt delta_iteration_count = iteration_count - iteration_count0;
  const HighsInt delta_dual_phase1_iteration_count =
      info.dual_phase1_iteration_count - dual_phase1_iteration_count0;
  const HighsInt delta_dual_phase2_iteration_count =
      info.dual_phase2_iteration_count - dual_phase2_iteration_count0;
  const HighsInt delta_primal_phase1_iteration_count =
      info.primal_phase1_iteration_count - primal_phase1_iteration_count0;
  const HighsInt delta_primal_phase2_iteration_count =
      info.primal_phase2_iteration_count - primal_phase2_iteration_count0;
  const HighsInt delta_primal_bound_swap =
      info.primal_bound_swap - primal_bound_swap0;

  HighsInt check_delta_iteration_count =
      delta_dual_phase1_iteration_count + delta_dual_phase2_iteration_count +
      delta_primal_phase1_iteration_count + delta_primal_phase2_iteration_count;
  if (check_delta_iteration_count != delta_iteration_count) {
    printf("Iteration total error %" HIGHSINT_FORMAT " + %" HIGHSINT_FORMAT
           " + %" HIGHSINT_FORMAT " + %" HIGHSINT_FORMAT " = %" HIGHSINT_FORMAT
           " != %" HIGHSINT_FORMAT "\n",
           delta_dual_phase1_iteration_count, delta_dual_phase2_iteration_count,
           delta_primal_phase1_iteration_count,
           delta_primal_phase2_iteration_count, check_delta_iteration_count,
           delta_iteration_count);
  }
  std::stringstream iteration_report;
  if (delta_dual_phase1_iteration_count) {
    iteration_report << "DuPh1 " << delta_dual_phase1_iteration_count << "; ";
  }
  if (delta_dual_phase2_iteration_count) {
    iteration_report << "DuPh2 " << delta_dual_phase2_iteration_count << "; ";
  }
  if (delta_primal_phase1_iteration_count) {
    iteration_report << "PrPh1 " << delta_primal_phase1_iteration_count << "; ";
  }
  if (delta_primal_phase2_iteration_count) {
    iteration_report << "PrPh2 " << delta_primal_phase2_iteration_count << "; ";
  }
  if (delta_primal_bound_swap) {
    iteration_report << "PrSwap " << delta_primal_bound_swap << "; ";
  }

  highsLogDev(log_options, HighsLogType::kInfo,
              "Simplex iterations: %sTotal %" HIGHSINT_FORMAT "\n",
              iteration_report.str().c_str(), delta_iteration_count);
}
