#ifndef STAN_MATH_REV_MAT_FUNCTOR_CVODES_UTILS_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_CVODES_UTILS_HPP

#include <cvodes/cvodes.h>
#include <sstream>
#include <stdexcept>

namespace stan {
namespace math {

// no-op error handler to silence CVodes error output;  errors handled
// directly by Stan
extern "C" inline void cvodes_silent_err_handler(int error_code,
                                                 const char* module,
                                                 const char* function,
                                                 char* msg, void* eh_data) {}

inline void cvodes_check_flag(int flag, const char* func_name) {
  if (flag < 0) {
    std::ostringstream ss;
    ss << func_name << " failed with error flag " << flag;
    throw std::runtime_error(ss.str());
  }
}

inline void cvodes_set_options(void* cvodes_mem, double rel_tol, double abs_tol,
                               // NOLINTNEXTLINE(runtime/int)
                               long int max_num_steps) {
  // forward CVode errors to noop error handler
  CVodeSetErrHandlerFn(cvodes_mem, cvodes_silent_err_handler, nullptr);

  // Initialize solver parameters
  cvodes_check_flag(CVodeSStolerances(cvodes_mem, rel_tol, abs_tol),
                    "CVodeSStolerances");

  cvodes_check_flag(CVodeSetMaxNumSteps(cvodes_mem, max_num_steps),
                    "CVodeSetMaxNumSteps");

  double init_step = 0;
  cvodes_check_flag(CVodeSetInitStep(cvodes_mem, init_step),
                    "CVodeSetInitStep");

  long int max_err_test_fails = 20;  // NOLINT(runtime/int)
  cvodes_check_flag(CVodeSetMaxErrTestFails(cvodes_mem, max_err_test_fails),
                    "CVodeSetMaxErrTestFails");

  long int max_conv_fails = 50;  // NOLINT(runtime/int)
  cvodes_check_flag(CVodeSetMaxConvFails(cvodes_mem, max_conv_fails),
                    "CVodeSetMaxConvFails");
}

}  // namespace math
}  // namespace stan
#endif
