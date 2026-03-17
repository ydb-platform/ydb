#ifndef STAN_SERVICES_UTIL_CREATE_UNIT_E_DIAG_INV_METRIC_HPP
#define STAN_SERVICES_UTIL_CREATE_UNIT_E_DIAG_INV_METRIC_HPP

#include <stan/io/dump.hpp>
#include <sstream>

namespace stan {
  namespace services {
    namespace util {

      /**
       * Create a stan::dump object which contains vector "metric"
       * of specified size where all elements are ones.
       *
       * @param[in] num_params expected number of diagonal elements
       * @return var_context 
       */
      inline
      stan::io::dump
      create_unit_e_diag_inv_metric(size_t num_params) {
        std::stringstream txt;
        txt << "inv_metric <- structure(c(";
        for (size_t i = 0; i < num_params; ++i) {
          txt << "1.0";
          if (i < num_params - 1)
            txt << ", ";
        }
        txt << "),.Dim=c(" << num_params << "))";
        return stan::io::dump(txt);
      }
    }
  }
}

#endif
