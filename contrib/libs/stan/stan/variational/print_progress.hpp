#ifndef STAN_VARIATIONAL_PRINT_PROGRESS_HPP
#define STAN_VARIATIONAL_PRINT_PROGRESS_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/math/prim/scal/err/check_positive.hpp>
#include <stan/math/prim/scal/err/check_nonnegative.hpp>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <string>

namespace stan {
  namespace variational {

    /**
     * Helper function for printing progress for variational inference
     *
     * @param[in] m       total number of iterations
     * @param[in] start   starting iteration
     * @param[in] finish  final iteration
     * @param[in] refresh how frequently we want to print an update
     * @param[in] tune    boolean indicates tuning vs. variational inference
     * @param[in] prefix  prefix string
     * @param[in] suffix  suffix string
     * @param[in,out] logger  logger
     */
    inline
    void print_progress(int m,
                        int start,
                        int finish,
                        int refresh,
                        bool tune,
                        const std::string& prefix,
                        const std::string& suffix,
                        callbacks::logger& logger) {
      static const char* function =
        "stan::variational::print_progress";

      stan::math::check_positive(function,
                                 "Total number of iterations",
                                 m);
      stan::math::check_nonnegative(function,
                                    "Starting iteration",
                                    start);
      stan::math::check_positive(function,
                                 "Final iteration",
                                 finish);
      stan::math::check_positive(function,
                                 "Refresh rate",
                                 refresh);

      int it_print_width = std::ceil(std::log10(static_cast<double>(finish)));
      if (refresh > 0
          && (start + m == finish || m - 1 == 0 || m % refresh == 0)) {
        std::stringstream ss;
        ss << prefix;
        ss << "Iteration: ";
        ss << std::setw(it_print_width) << m + start
           << " / " << finish;
        ss << " [" << std::setw(3)
           << (100 * (start + m)) / finish
           << "%] ";
        ss << (tune ? " (Adaptation)" : " (Variational Inference)");
        ss << suffix;
        logger.info(ss);
      }
    }

  }
}
#endif
