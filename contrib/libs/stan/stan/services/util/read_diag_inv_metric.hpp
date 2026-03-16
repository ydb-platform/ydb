#ifndef STAN_SERVICES_UTIL_READ_DIAG_INV_METRIC_HPP
#define STAN_SERVICES_UTIL_READ_DIAG_INV_METRIC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/io/var_context.hpp>
#include <Eigen/Dense>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace stan {
  namespace services {
    namespace util {

      /**
       * Extract diagonal values for an inverse Euclidean metric 
       * from a var_context object.
       *
       * @param[in] init_context a var_context with initial values
       * @param[in] num_params expected number of diagonal elements
       * @param[in,out] logger Logger for messages
       * @throws std::domain_error if the Euclidean metric is invalid
       * @return inv_metric vector of diagonal values
       */
      inline
      Eigen::VectorXd
      read_diag_inv_metric(stan::io::var_context& init_context,
                           size_t num_params,
                           callbacks::logger& logger) {
        Eigen::VectorXd inv_metric(num_params);
        try {
          init_context.validate_dims(
            "read diag inv metric", "inv_metric", "vector_d",
            init_context.to_vec(num_params));
          std::vector<double> diag_vals =
            init_context.vals_r("inv_metric");
          for (size_t i=0; i < num_params; i++) {
            inv_metric(i) = diag_vals[i];
          }
        } catch (const std::exception& e) {
          logger.error("Cannot get inverse Euclidean metric from input file.");
          logger.error("Caught exception: ");
          logger.error(e.what());
          throw std::domain_error("Initialization failure");
        }
        return inv_metric;
      }

    }
  }
}

#endif
