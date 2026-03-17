#ifndef STAN_VARIATIONAL_NORMAL_MEANFIELD_HPP
#define STAN_VARIATIONAL_NORMAL_MEANFIELD_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/math/prim/mat.hpp>
#include <stan/model/gradient.hpp>
#include <stan/variational/base_family.hpp>
#include <algorithm>
#include <ostream>
#include <vector>

namespace stan {

  namespace variational {

    /**
     * Variational family approximation with mean-field (diagonal
     * covariance) multivariate normal distribution.
     */
    class normal_meanfield : public base_family {
    private:
      /**
       * Mean vector.
       */
      Eigen::VectorXd mu_;

      /**
       * Log standard deviation (log scale) vector.
       */
      Eigen::VectorXd omega_;

      /**
       * Dimensionality of distribution.
       */
      const int dimension_;

    public:
      /**
       * Construct a variational distribution of the specified
       * dimensionality with a zero mean and zero log standard
       * deviation (unit standard deviation).
       *
       * @param[in] dimension Dimensionality of distribution.
       */
      explicit normal_meanfield(size_t dimension)
        : mu_(Eigen::VectorXd::Zero(dimension)),
          omega_(Eigen::VectorXd::Zero(dimension)),
          dimension_(dimension) {
      }

      /**
       * Construct a variational distribution with the specified mean
       * vector and zero log standard deviation (unit standard
       * deviation).
       *
       * @param[in] cont_params Mean vector.
       */
      explicit normal_meanfield(const Eigen::VectorXd& cont_params)
        : mu_(cont_params),
          omega_(Eigen::VectorXd::Zero(cont_params.size())),
          dimension_(cont_params.size()) {
      }

      /**
       * Construct a variational distribution with the specified mean
       * and log standard deviation vectors.
       *
       * @param[in] mu Mean vector.
       * @param[in] omega Log standard deviation vector.
       * @throw std::domain_error If the sizes of mean and log
       * standard deviation vectors are different, or if either
       * contains a not-a-number value.
       */
      normal_meanfield(const Eigen::VectorXd& mu,
                       const Eigen::VectorXd& omega)
        : mu_(mu), omega_(omega), dimension_(mu.size()) {
        static const char* function =
          "stan::variational::normal_meanfield";
        stan::math::check_size_match(function,
                             "Dimension of mean vector", mu_.size(),
                             "Dimension of log std vector", omega_.size() );
        stan::math::check_not_nan(function, "Mean vector", mu_);
        stan::math::check_not_nan(function, "Log std vector", omega_);
      }

      /**
       * Return the dimensionality of the approximation.
       */
      int dimension() const { return dimension_; }

      /**
       * Return the mean vector.
       */
      const Eigen::VectorXd& mu() const { return mu_; }

      /**
       * Return the log standard deviation vector.
       */
      const Eigen::VectorXd& omega() const { return omega_; }

      /**
       * Set the mean vector to the specified value.
       *
       * @param[in] mu Mean vector.
       * @throw std::domain_error If the mean vector's size does not
       * match this approximation's dimensionality, or if it contains
       * not-a-number values.
       */
      void set_mu(const Eigen::VectorXd& mu) {
        static const char* function =
          "stan::variational::normal_meanfield::set_mu";

        stan::math::check_size_match(function,
                               "Dimension of input vector", mu.size(),
                               "Dimension of current vector", dimension());
        stan::math::check_not_nan(function, "Input vector", mu);
        mu_ = mu;
      }

      /**
       * Set the log standard deviation vector to the specified
       * value.
       *
       * @param[in] omega Log standard deviation vector.
       * @throw std::domain_error If the log standard deviation
       * vector's size does not match this approximation's
       * dimensionality, or if it contains not-a-number values.
       */
      void set_omega(const Eigen::VectorXd& omega) {
        static const char* function =
          "stan::variational::normal_meanfield::set_omega";

        stan::math::check_size_match(function,
                               "Dimension of input vector", omega.size(),
                               "Dimension of current vector", dimension());
        stan::math::check_not_nan(function, "Input vector", omega);
        omega_ = omega;
      }

      /**
       * Sets the mean and log standard deviation vector for this
       * approximation to zero.
       */
      void set_to_zero() {
        mu_ = Eigen::VectorXd::Zero(dimension());
        omega_ = Eigen::VectorXd::Zero(dimension());
      }

      /**
       * Return a new mean field approximation resulting from squaring
       * the entries in the mean and log standard deviation.  The new
       * approximation does not hold any references to this
       * approximation.
       */
      normal_meanfield square() const {
        return normal_meanfield(Eigen::VectorXd(mu_.array().square()),
                                Eigen::VectorXd(omega_.array().square()));
      }

      /**
       * Return a new mean field approximation resulting from taking
       * the square root of the entries in the mean and log standard
       * deviation.  The new approximation does not hold any
       * references to this approximation.
       *
       * <b>Warning:</b>  No checks are carried out to ensure the
       * entries are non-negative before taking square roots, so
       * not-a-number values may result.
       */
      normal_meanfield sqrt() const {
        return normal_meanfield(Eigen::VectorXd(mu_.array().sqrt()),
                                Eigen::VectorXd(omega_.array().sqrt()));
      }

      /**
       * Return this approximation after setting its mean vector and
       * Cholesky factor for covariance to the values given by the
       * specified approximation.
       *
       * @param[in] rhs Approximation from which to gather the mean
       * and log standard deviation vectors.
       * @return This approximation after assignment.
       * @throw std::domain_error If the dimensionality of the specified
       * approximation does not match this approximation's dimensionality.
       */
      normal_meanfield& operator=(const normal_meanfield& rhs) {
        static const char* function =
          "stan::variational::normal_meanfield::operator=";
        stan::math::check_size_match(function,
                             "Dimension of lhs", dimension(),
                             "Dimension of rhs", rhs.dimension());
        mu_ = rhs.mu();
        omega_ = rhs.omega();
        return *this;
      }

      /**
       * Add the mean and Cholesky factor of the covariance matrix of
       * the specified approximation to this approximation.
       *
       * @param[in] rhs Approximation from which to gather the mean
       * and log standard deviation vectors.
       * @return This approximation after adding the specified
       * approximation.
       * @throw std::domain_error If the size of the specified
       * approximation does not match the size of this approximation.
       */
      normal_meanfield& operator+=(const normal_meanfield& rhs) {
        static const char* function =
          "stan::variational::normal_meanfield::operator+=";
        stan::math::check_size_match(function,
                             "Dimension of lhs", dimension(),
                             "Dimension of rhs", rhs.dimension());
        mu_ += rhs.mu();
        omega_ += rhs.omega();
        return *this;
      }

      /**
       * Return this approximation after elementwise division by the
       * specified approximation's mean and log standard deviation
       * vectors.
       *
       * @param[in] rhs Approximation from which to gather the mean
       * and log standard deviation vectors.
       * @return This approximation after elementwise division by the
       * specified approximation.
       * @throw std::domain_error If the dimensionality of the specified
       * approximation does not match this approximation's dimensionality.
       */
      inline
      normal_meanfield& operator/=(const normal_meanfield& rhs) {
        static const char* function =
          "stan::variational::normal_meanfield::operator/=";
        stan::math::check_size_match(function,
                             "Dimension of lhs", dimension(),
                             "Dimension of rhs", rhs.dimension());
        mu_.array() /= rhs.mu().array();
        omega_.array() /= rhs.omega().array();
        return *this;
      }

      /**
       * Return this approximation after adding the specified scalar
       * to each entry in the mean and log standard deviation vectors.
       *
       * <b>Warning:</b> No finiteness check is made on the scalar, so
       * it may introduce NaNs.
       *
       * @param[in] scalar Scalar to add.
       * @return This approximation after elementwise addition of the
       * specified scalar.
       */
      normal_meanfield& operator+=(double scalar) {
        mu_.array() += scalar;
        omega_.array() += scalar;
        return *this;
      }

      /**
       * Return this approximation after multiplying by the specified
       * scalar to each entry in the mean and log standard deviation
       * vectors.
       *
       * <b>Warning:</b> No finiteness check is made on the scalar, so
       * it may introduce NaNs.
       *
       * @param[in] scalar Scalar to add.
       * @return This approximation after elementwise addition of the
       * specified scalar.
       */
      normal_meanfield& operator*=(double scalar) {
        mu_ *= scalar;
        omega_ *= scalar;
        return *this;
      }

      /**
       * Returns the mean vector for this approximation.
       *
       * See: <code>mu()</code>.
       *
       * @return Mean vector for this approximation.
       */
      const Eigen::VectorXd& mean() const {
        return mu();
      }

      /**
       * Return the entropy of the approximation.
       *
       * <p>The entropy is defined by
       *   0.5 * dim * (1+log2pi) + 0.5 * log det diag(sigma^2)
       * = 0.5 * dim * (1+log2pi) + sum(log(sigma))
       * = 0.5 * dim * (1+log2pi) + sum(omega)
       *
       * @return Entropy of this approximation.
       */
      double entropy() const {
        return 0.5 * static_cast<double>(dimension()) *
               (1.0 + stan::math::LOG_TWO_PI) + omega_.sum();
      }

      /**
       * Return the transform of the sepcified vector using the
       * Cholesky factor and mean vector.
       *
       * The transform is defined by
       * S^{-1}(eta) = sigma * eta + mu = exp(omega) * eta + mu.
       *
       * @param[in] eta Vector to transform.
       * @throw std::domain_error If the specified vector's size does
       * not match the dimensionality of this approximation.
       * @return Transformed vector.
       */
      Eigen::VectorXd transform(const Eigen::VectorXd& eta) const {
        static const char* function =
          "stan::variational::normal_meanfield::transform";
        stan::math::check_size_match(function,
                         "Dimension of mean vector", dimension(),
                         "Dimension of input vector", eta.size() );
        stan::math::check_not_nan(function, "Input vector", eta);
        // exp(omega) * eta + mu
        return eta.array().cwiseProduct(omega_.array().exp()) + mu_.array();
      }

      /**
       * Calculates the "blackbox" gradient with respect to both the
       * location vector (mu) and the log-std vector (omega) in
       * parallel.  It uses the same gradient computed from a set of
       * Monte Carlo samples.
       *
       * @tparam M Model class.
       * @tparam BaseRNG Class of base random number generator.
       * @param[in] elbo_grad Parameters to store "blackbox" gradient
       * @param[in] m Model.
       * @param[in] cont_params Continuous parameters.
       * @param[in] n_monte_carlo_grad Number of samples for gradient
       * computation.
       * @param[in,out] rng Random number generator.
       * @param[in,out] logger logger for messages
       * @throw std::domain_error If the number of divergent
       * iterations exceeds its specified bounds.
       */
      template <class M, class BaseRNG>
      void calc_grad(normal_meanfield& elbo_grad,
                     M& m,
                     Eigen::VectorXd& cont_params,
                     int n_monte_carlo_grad,
                     BaseRNG& rng,
                     callbacks::logger& logger)
        const {
        static const char* function =
          "stan::variational::normal_meanfield::calc_grad";

        stan::math::check_size_match(function,
                        "Dimension of elbo_grad", elbo_grad.dimension(),
                        "Dimension of variational q", dimension());
        stan::math::check_size_match(function,
                        "Dimension of variational q", dimension(),
                        "Dimension of variables in model", cont_params.size());

        Eigen::VectorXd mu_grad    = Eigen::VectorXd::Zero(dimension());
        Eigen::VectorXd omega_grad = Eigen::VectorXd::Zero(dimension());
        double tmp_lp = 0.0;
        Eigen::VectorXd tmp_mu_grad = Eigen::VectorXd::Zero(dimension());
        Eigen::VectorXd eta  = Eigen::VectorXd::Zero(dimension());
        Eigen::VectorXd zeta = Eigen::VectorXd::Zero(dimension());

        // Naive Monte Carlo integration
        static const int n_retries = 10;
        for (int i = 0, n_monte_carlo_drop = 0; i < n_monte_carlo_grad; ) {
          // Draw from standard normal and transform to real-coordinate space
          for (int d = 0; d < dimension(); ++d)
            eta(d) = stan::math::normal_rng(0, 1, rng);
          zeta = transform(eta);
          try {
            std::stringstream ss;
            stan::model::gradient(m, zeta, tmp_lp, tmp_mu_grad, &ss);
            if (ss.str().length() > 0)
              logger.info(ss);
            stan::math::check_finite(function, "Gradient of mu", tmp_mu_grad);
            mu_grad += tmp_mu_grad;
            omega_grad.array() += tmp_mu_grad.array().cwiseProduct(eta.array());
            ++i;
          } catch (const std::exception& e) {
            ++n_monte_carlo_drop;
            if (n_monte_carlo_drop >= n_retries * n_monte_carlo_grad) {
              const char* name = "The number of dropped evaluations";
              const char* msg1 = "has reached its maximum amount (";
              int y = n_retries * n_monte_carlo_grad;
              const char* msg2 = "). Your model may be either severely "
                "ill-conditioned or misspecified.";
              stan::math::domain_error(function, name, y, msg1, msg2);
            }
          }
        }
        mu_grad /= static_cast<double>(n_monte_carlo_grad);
        omega_grad /= static_cast<double>(n_monte_carlo_grad);

        omega_grad.array()
          = omega_grad.array().cwiseProduct(omega_.array().exp());

        omega_grad.array() += 1.0;  // add entropy gradient (unit)

        elbo_grad.set_mu(mu_grad);
        elbo_grad.set_omega(omega_grad);
      }
    };

    /**
     * Return a new approximation resulting from adding the mean and
     * log standard deviation of the specified approximations.
     *
     * @param[in] lhs First approximation.
     * @param[in] rhs Second approximation.
     * @return Sum of the specified approximations.
     * @throw std::domain_error If the dimensionalities do not match.
     */
    inline
    normal_meanfield operator+(normal_meanfield lhs,
                               const normal_meanfield& rhs) {
      return lhs += rhs;
    }

    /**
     * Return a new approximation resulting from elementwise division of
     * of the first specified approximation by the second.
     *
     * @param[in] lhs First approximation.
     * @param[in] rhs Second approximation.
     * @return Elementwise division of the specified approximations.
     * @throw std::domain_error If the dimensionalities do not match.
     */
    inline
    normal_meanfield operator/(normal_meanfield lhs,
                               const normal_meanfield& rhs) {
      return lhs /= rhs;
    }

    /**
     * Return a new approximation resulting from elementwise addition
     * of the specified scalar to the mean and log standard deviation
     * entries of the specified approximation.
     *
     * @param[in] scalar Scalar value
     * @param[in] rhs Approximation.
     * @return Addition of scalar to specified approximation.
     */
    inline
    normal_meanfield operator+(double scalar, normal_meanfield rhs) {
      return rhs += scalar;
    }

    /**
     * Return a new approximation resulting from elementwise
     * multiplication of the specified scalar to the mean and log
     * standard deviation vectors of the specified approximation.
     *
     * @param[in] scalar Scalar value
     * @param[in] rhs Approximation.
     * @return Multiplication of scalar by the specified approximation.
     */
    inline
    normal_meanfield operator*(double scalar, normal_meanfield rhs) {
      return rhs *= scalar;
    }

  }
}
#endif
