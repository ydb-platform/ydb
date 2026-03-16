#ifndef STAN_SERVICES_UTIL_READ_DENSE_INV_METRIC_HPP
#define STAN_SERVICES_UTIL_READ_DENSE_INV_METRIC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/io/var_context.hpp>
#include <stan/math/prim/mat.hpp>
#include <limits>
#include <sstream>
#include <string>
#include <vector>

namespace stan {
  namespace services {
    namespace util {

      /**
       * Extract dense inverse Euclidean metric from a var_context object.
       *
       * @param[in] init_context a var_context with array of initial values
       * @param[in] num_params expected number of row, column elements
       * @param[in,out] logger Logger for messages
       * @throws std::domain_error if cannot read the Euclidean metric
       * @return inv_metric
       */
      inline
      Eigen::MatrixXd
      read_dense_inv_metric(stan::io::var_context& init_context,
                            size_t num_params,
                            callbacks::logger& logger) {
        Eigen::MatrixXd inv_metric;
        try {
          init_context.validate_dims(
            "read dense inv metric", "inv_metric", "matrix",
            init_context.to_vec(num_params, num_params));
          std::vector<double> dense_vals =
            init_context.vals_r("inv_metric");
          inv_metric =
            stan::math::to_matrix(dense_vals, num_params, num_params);
        } catch (const std::exception& e) {
          logger.error("Cannot get inverse metric from input file.");
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
