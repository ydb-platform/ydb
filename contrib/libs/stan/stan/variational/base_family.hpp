#ifndef STAN_VARIATIONAL_BASE_FAMILY_HPP
#define STAN_VARIATIONAL_BASE_FAMILY_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/math/prim/mat.hpp>
#include <algorithm>
#include <ostream>

namespace stan {
  namespace variational {

    class base_family {
    public:
      // Constructors
      base_family() {}

      /**
       * Return the dimensionality of the approximation.
       */
      virtual int dimension() const = 0;

      // Distribution-based operations
      virtual const Eigen::VectorXd& mean() const = 0;
      virtual double entropy() const  = 0;
      virtual Eigen::VectorXd transform(const Eigen::VectorXd& eta) const = 0;
      /**
       * Assign a draw from this mean field approximation to the
       * specified vector using the specified random number generator.
       *
       * @tparam BaseRNG Class of random number generator.
       * @param[in] rng Base random number generator.
       * @param[out] eta Vector to which the draw is assigned; dimension has to be
       * the same as the dimension of variational q.
       * @throws std::range_error If the index is out of range.
       */
      template <class BaseRNG>
      void sample(BaseRNG& rng, Eigen::VectorXd& eta) const {
        // Draw from standard normal and transform to real-coordinate space
        for (int d = 0; d < dimension(); ++d)
          eta(d) = stan::math::normal_rng(0, 1, rng);
        eta = transform(eta);
      }
      /**
       * Draw a posterior sample from the normal distribution,
       * and return its log normal density. The constants are dropped.
       *
       * @param[in] rng Base random number generator.
       * @param[out] eta Vector to which the draw is assigned; dimension has to be
       * the same as the dimension of variational q. eta will be transformed into
       * variational posteriors.
       * @param[out] log_g The log  density in the variational approximation;
       * The constant term is dropped.
       * @throws std::range_error If the index is out of range.
       */
      template <class BaseRNG>
      void sample_log_g(BaseRNG& rng,
                        Eigen::VectorXd& eta,
                        double& log_g) const {
        // Draw from the approximation
        for (int d = 0; d < dimension(); ++d) {
          eta(d) = stan::math::normal_rng(0, 1, rng);
        }
        // Compute the log density before transformation
        log_g = calc_log_g(eta);
        // Transform to real-coordinate space
        eta = transform(eta);
      }
      /**
       * Compute the unnormalized log unit normal density wrt eta. All constants are dropped.
       *
       * @param[in] eta Vector; dimension has to be the same as the dimension
       * of variational q.
       * @return The unnormalized log density in the variational approximation;
       * @throws std::range_error If the index is out of range.
       */
      double calc_log_g(const Eigen::VectorXd& eta) const {
        double log_g = 0;
        for (int d = 0; d < dimension(); ++d) {
          log_g += -stan::math::square(eta(d)) * 0.5;
        }
        return log_g;
      }
      template <class M, class BaseRNG>
      void calc_grad(base_family& elbo_grad,
                     M& m,
                     Eigen::VectorXd& cont_params,
                     int n_monte_carlo_grad,
                     BaseRNG& rng,
                     callbacks::logger& logger)
        const;

    protected:
      void write_error_msg_(std::ostream* error_msgs,
                            const std::exception& e) const {
        if (!error_msgs) {
          return;
        }

        *error_msgs
          << std::endl
          << "Informational Message: The current gradient evaluation "
          << "of the ELBO is ignored because of the following issue:"
          << std::endl
          << e.what() << std::endl
          << "If this warning occurs often then your model may be "
          << "either severely ill-conditioned or misspecified."
          << std::endl;
      }
    };

  }  // variational
}  // stan
#endif
