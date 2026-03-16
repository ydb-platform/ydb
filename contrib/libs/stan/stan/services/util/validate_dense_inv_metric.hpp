#ifndef STAN_SERVICES_UTIL_VALIDATE_DENSE_INV_METRIC_HPP
#define STAN_SERVICES_UTIL_VALIDATE_DENSE_INV_METRIC_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/math/prim/mat.hpp>

namespace stan {
  namespace services {
    namespace util {

      /**
       * Validate that dense inverse Euclidean metric is positive definite
       *
       * @param[in] inv_metric  inverse Euclidean metric
       * @param[in,out] logger Logger for messages
       * @throws std::domain_error if matrix is not positive definite
       */
      inline
      void
      validate_dense_inv_metric(const Eigen::MatrixXd& inv_metric,
                                callbacks::logger& logger) {
        try {
          stan::math::check_pos_definite("check_pos_definite",
                                         "inv_metric", inv_metric);
        } catch (const std::domain_error& e) {
          logger.error("Inverse Euclidean metric not positive definite.");
          throw std::domain_error("Initialization failure");
        }
      }

    }
  }
}

#endif
