#ifndef STAN_VARIATIONAL_NORMAL_FULLRANK_HPP
#define STAN_VARIATIONAL_NORMAL_FULLRANK_HPP

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
     * Variational family approximation with full-rank multivariate
     * normal distribution.
     */
    class normal_fullrank : public base_family {
    private:
      /**
       * Mean vector.
       */
      Eigen::VectorXd mu_;

      /**
       * Cholesky factor of covariance:
       * Sigma = L_chol *  L_chol.transpose()
       */
      Eigen::MatrixXd L_chol_;

      /**
       * Dimensionality of distribution.
       */
      const int dimension_;

      /**
       * Raise a domain exception if the specified vector contains
       * not-a-number values.
       *
       * @param[in] mu Mean vector.
       * @throw std::domain_error If the mean vector contains NaN
       * values or does not match this distribution's dimensionality.
       */
      void validate_mean(const char* function,
                                       const Eigen::VectorXd& mu) {
        stan::math::check_not_nan(function, "Mean vector", mu);
        stan::math::check_size_match(function,
                               "Dimension of input vector", mu.size(),
                               "Dimension of current vector", dimension());
      }

      /**
       * Raise a domain exception if the specified matrix is not
       * square, not lower triangular, or contains not-a-number
       * values.
       *
       * <b>Warning:</b> This function does not check that the
       * Cholesky factor is positive definite.
       *
       * @param[in] L_chol Cholesky factor for covariance matrix.
       * @throw std::domain_error If the specified matrix is not
       * square, is not lower triangular, if its size does not match
       * the dimensionality of this approximation, or if it contains
       * not-a-number values.
       */
      void validate_cholesky_factor(const char* function,
                                    const Eigen::MatrixXd& L_chol) {
        stan::math::check_square(function, "Cholesky factor", L_chol);
        stan::math::check_lower_triangular(function,
                               "Cholesky factor", L_chol);
        stan::math::check_size_match(function,
                               "Dimension of mean vector", dimension(),
                               "Dimension of Cholesky factor", L_chol.rows());
        stan::math::check_not_nan(function, "Cholesky factor", L_chol);
      }


    public:
      /**
       * Construct a variational distribution of the specified
       * dimensionality with a zero mean and Cholesky factor of a zero
       * covariance matrix.
       *
       * @param[in] dimension Dimensionality of distribution.
       */
      explicit normal_fullrank(size_t dimension)
      : mu_(Eigen::VectorXd::Zero(dimension)),
        L_chol_(Eigen::MatrixXd::Zero(dimension, dimension)),
        dimension_(dimension) {
      }


      /**
       * Construct a variational distribution with specified mean vector
       * and Cholesky factor for identity covariance.
       *
       * @param[in] cont_params Mean vector.
       */
      explicit normal_fullrank(const Eigen::VectorXd& cont_params)
      : mu_(cont_params),
        L_chol_(Eigen::MatrixXd::Identity(cont_params.size(),
                                          cont_params.size())),
        dimension_(cont_params.size()) {
      }

      /**
       * Construct a variational distribution with specified mean and
       * Cholesky factor for covariance.
       *
       * <b>Warning</b>: Positive-definiteness is not enforced for the
       * Cholesky factor.
       *
       * @param[in] mu Mean vector.
       * @param[in] L_chol Cholesky factor of covariance.
       * @throws std::domain_error If the Cholesky factor is not
       * square or not lower triangular, if the mean and Cholesky factor
       * have different dimensionality, or if any of the elements is
       * not-a-number.
       */
      normal_fullrank(const Eigen::VectorXd& mu,
                      const Eigen::MatrixXd& L_chol)
      : mu_(mu), L_chol_(L_chol),  dimension_(mu.size()) {
        static const char* function = "stan::variational::normal_fullrank";
        validate_mean(function, mu);
        validate_cholesky_factor(function, L_chol);
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
       * Return the Cholesky factor of the covariance matrix.
       */
      const Eigen::MatrixXd& L_chol() const { return L_chol_; }

      /**
       * Set the mean vector to the specified value.
       *
       * @param[in] mu Mean vector.
       * @throw std::domain_error If the size of the specified mean
       * vector does not match the stored dimension of this approximation.
       */
      void set_mu(const Eigen::VectorXd& mu) {
        static const char* function = "stan::variational::set_mu";
        validate_mean(function, mu);
        mu_ = mu;
      }

      /**
       * Set the Cholesky factor to the specified value.
       *
       * @param[in] L_chol Cholesky factor of covariance matrix.
       * @throw std::domain_error  If the specified matrix is not
       * square, is not lower triangular, if its size does not match
       * the dimensionality of this approximation, or if it contains
       * not-a-number values.
       */
      void set_L_chol(const Eigen::MatrixXd& L_chol) {
        static const char* function = "stan::variational::set_L_chol";
        validate_cholesky_factor(function, L_chol);
        L_chol_ = L_chol;
      }

      /**
       * Set the mean vector and Cholesky factor for the covariance
       * matrix to zero.
       */
      void set_to_zero() {
        mu_ = Eigen::VectorXd::Zero(dimension());
        L_chol_ = Eigen::MatrixXd::Zero(dimension(), dimension());
      }

      /**
       * Return a new full rank approximation resulting from squaring
       * the entries in the mean and Cholesky factor for the
       * covariance matrix.  The new approximation does not hold
       * any references to this approximation.
       */
      normal_fullrank square() const {
        return normal_fullrank(Eigen::VectorXd(mu_.array().square()),
                               Eigen::MatrixXd(L_chol_.array().square()));
      }

      /**
       * Return a new full rank approximation resulting from taking
       * the square root of the entries in the mean and Cholesky
       * factor for the covariance matrix.  The new approximation does
       * not hold any references to this approximation.
       *
       * <b>Warning:</b>  No checks are carried out to ensure the
       * entries are non-negative before taking square roots, so
       * not-a-number values may result.
       */
      normal_fullrank sqrt() const {
        return normal_fullrank(Eigen::VectorXd(mu_.array().sqrt()),
                               Eigen::MatrixXd(L_chol_.array().sqrt()));
      }

      /**
       * Return this approximation after setting its mean vector and
       * Cholesky factor for covariance to the values given by the
       * specified approximation.
       *
       * @param[in] rhs Approximation from which to gather the mean and
       * covariance.
       * @return This approximation after assignment.
       * @throw std::domain_error If the dimensionality of the specified
       * approximation does not match this approximation's dimensionality.
       */
      normal_fullrank& operator=(const normal_fullrank& rhs) {
        static const char* function =
          "stan::variational::normal_fullrank::operator=";
        stan::math::check_size_match(function,
                             "Dimension of lhs", dimension(),
                             "Dimension of rhs", rhs.dimension());
        mu_ = rhs.mu();
        L_chol_ = rhs.L_chol();
        return *this;
      }

      /**
       * Add the mean and Cholesky factor of the covariance matrix of
       * the specified approximation to this approximation.
       *
       * @param[in] rhs Approximation from which to gather the mean and
       * covariance.
       * @return This approximation after adding the specified
       * approximation.
       * @throw std::domain_error If the dimensionality of the specified
       * approximation does not match this approximation's dimensionality.
       */
      normal_fullrank& operator+=(const normal_fullrank& rhs) {
        static const char* function =
          "stan::variational::normal_fullrank::operator+=";
        stan::math::check_size_match(function,
                             "Dimension of lhs", dimension(),
                             "Dimension of rhs", rhs.dimension());
        mu_ += rhs.mu();
        L_chol_ += rhs.L_chol();
        return *this;
      }

      /**
       * Return this approximation after elementwise division by the
       * specified approximation's mean and Cholesky factor for
       * covariance.
       *
       * @param[in] rhs Approximation from which to gather the mean and
       * covariance.
       * @return This approximation after elementwise division by the
       * specified approximation.
       * @throw std::domain_error If the dimensionality of the specified
       * approximation does not match this approximation's dimensionality.
       */
      inline
      normal_fullrank& operator/=(const normal_fullrank& rhs) {
        static const char* function =
          "stan::variational::normal_fullrank::operator/=";

        stan::math::check_size_match(function,
                             "Dimension of lhs", dimension(),
                             "Dimension of rhs", rhs.dimension());

        mu_.array() /= rhs.mu().array();
        L_chol_.array() /= rhs.L_chol().array();
        return *this;
      }

      /**
       * Return this approximation after adding the specified scalar
       * to each entry in the mean and cholesky factor for covariance.
       *
       * <b>Warning:</b> No finiteness check is made on the scalar, so
       * it may introduce NaNs.
       *
       * @param[in] scalar Scalar to add.
       * @return This approximation after elementwise addition of the
       * specified scalar.
       */
      normal_fullrank& operator+=(double scalar) {
        mu_.array() += scalar;
        L_chol_.array() += scalar;
        return *this;
      }

      /**
       * Return this approximation after multiplying by the specified
       * scalar to each entry in the mean and cholesky factor for
       * covariance.
       *
       * <b>Warning:</b> No finiteness check is made on the scalar, so
       * it may introduce NaNs.
       *
       * @param[in] scalar Scalar to add.
       * @return This approximation after elementwise addition of the
       * specified scalar.
       */
      normal_fullrank& operator*=(double scalar) {
        mu_ *= scalar;
        L_chol_ *= scalar;
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
       * Return the entropy of this approximation.
       *
       * <p>The entropy is defined by
       * 0.5 * dim * (1+log2pi) + 0.5 * log det (L^T L)
       * = 0.5 * dim * (1+log2pi) + sum(log(abs(diag(L)))).
       *
       * @return Entropy of this approximation
       */
      double entropy() const {
        static double mult = 0.5 * (1.0 + stan::math::LOG_TWO_PI);
        double result = mult * dimension();
        for (int d = 0; d < dimension(); ++d) {
          double tmp = fabs(L_chol_(d, d));
          if (tmp != 0.0) result += log(tmp);
        }
        return result;
      }

      /**
       * Return the transform of the sepcified vector using the
       * Cholesky factor and mean vector.
       *
       * The transform is defined by
       * S^{-1}(eta) = L_chol * eta + mu.
       *
       * @param[in] eta Vector to transform.
       * @throw std::domain_error If the specified vector's size does
       * not match the dimensionality of this approximation.
       * @return Transformed vector.
       */
      Eigen::VectorXd transform(const Eigen::VectorXd& eta) const {
        static const char* function =
          "stan::variational::normal_fullrank::transform";
        stan::math::check_size_match(function,
                         "Dimension of input vector", eta.size(),
                         "Dimension of mean vector",  dimension());
        stan::math::check_not_nan(function, "Input vector", eta);

        return (L_chol_ * eta) + mu_;
      }

      template <class BaseRNG>
      void sample(BaseRNG& rng, Eigen::VectorXd& eta) const {
        // Draw from standard normal and transform to real-coordinate space
        for (int d = 0; d < dimension(); ++d)
          eta(d) = stan::math::normal_rng(0, 1, rng);
        eta = transform(eta);
      }

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

      double calc_log_g(const Eigen::VectorXd& eta) const {
        // Compute the log density wrt normal distribution dropping constants
        double log_g = 0;
        for (int d = 0; d < dimension(); ++d) {
          log_g += -stan::math::square(eta(d)) * 0.5;
        }
        return log_g;
      }

      /**
       * Calculates the "blackbox" gradient with respect to BOTH the
       * location vector (mu) and the cholesky factor of the scale
       * matrix (L_chol) in parallel. It uses the same gradient
       * computed from a set of Monte Carlo samples
       *
       * @tparam M Model class.
       * @tparam BaseRNG Class of base random number generator.
       * @param[in] elbo_grad Approximation to store "blackbox" gradient.
       * @param[in] m Model.
       * @param[in] cont_params Continuous parameters.
       * @param[in] n_monte_carlo_grad Sample size for gradient computation.
       * @param[in,out] rng Random number generator.
       * @param[in,out] logger logger for messages
       * @throw std::domain_error If the number of divergent
       * iterations exceeds its specified bounds.
       */
      template <class M, class BaseRNG>
      void calc_grad(normal_fullrank& elbo_grad,
                     M& m,
                     Eigen::VectorXd& cont_params,
                     int n_monte_carlo_grad,
                     BaseRNG& rng,
                     callbacks::logger& logger)
        const {
        static const char* function =
          "stan::variational::normal_fullrank::calc_grad";
        stan::math::check_size_match(function,
                        "Dimension of elbo_grad", elbo_grad.dimension(),
                        "Dimension of variational q", dimension());
        stan::math::check_size_match(function,
                        "Dimension of variational q", dimension(),
                        "Dimension of variables in model", cont_params.size());

        Eigen::VectorXd mu_grad = Eigen::VectorXd::Zero(dimension());
        Eigen::MatrixXd L_grad  = Eigen::MatrixXd::Zero(dimension(),
                                                        dimension());
        double tmp_lp = 0.0;
        Eigen::VectorXd tmp_mu_grad = Eigen::VectorXd::Zero(dimension());
        Eigen::VectorXd eta = Eigen::VectorXd::Zero(dimension());
        Eigen::VectorXd zeta = Eigen::VectorXd::Zero(dimension());

        // Naive Monte Carlo integration
        static const int n_retries = 10;
        for (int i = 0, n_monte_carlo_drop = 0; i < n_monte_carlo_grad; ) {
          // Draw from standard normal and transform to real-coordinate space
          for (int d = 0; d < dimension(); ++d) {
            eta(d) = stan::math::normal_rng(0, 1, rng);
          }
          zeta = transform(eta);
          try {
            std::stringstream ss;
            stan::model::gradient(m, zeta, tmp_lp, tmp_mu_grad, &ss);
            if (ss.str().length() > 0)
              logger.info(ss);
            stan::math::check_finite(function, "Gradient of mu", tmp_mu_grad);

            mu_grad += tmp_mu_grad;
            for (int ii = 0; ii < dimension(); ++ii) {
              for (int jj = 0; jj <= ii; ++jj) {
                L_grad(ii, jj) += tmp_mu_grad(ii) * eta(jj);
              }
            }
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
        L_grad  /= static_cast<double>(n_monte_carlo_grad);

        // Add gradient of entropy term
        L_grad.diagonal().array() += L_chol_.diagonal().array().inverse();

        elbo_grad.set_mu(mu_grad);
        elbo_grad.set_L_chol(L_grad);
      }
    };

    /**
     * Return a new approximation resulting from adding the mean and
     * covariance matrix Cholesky factor of the specified
     * approximations.
     *
     * @param[in] lhs First approximation.
     * @param[in] rhs Second approximation.
     * @return Sum of the specified approximations.
     * @throw std::domain_error If the dimensionalities do not match.
     */
    inline
    normal_fullrank operator+(normal_fullrank lhs, const normal_fullrank& rhs) {
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
    normal_fullrank operator/(normal_fullrank lhs, const normal_fullrank& rhs) {
      return lhs /= rhs;
    }

    /**
     * Return a new approximation resulting from elementwise addition
     * of the specified scalar to the mean and Cholesky factor of
     * covariance entries for the specified approximation.
     *
     * @param[in] scalar Scalar value
     * @param[in] rhs Approximation.
     * @return Addition of scalar to specified approximation.
     */
    inline
    normal_fullrank operator+(double scalar, normal_fullrank rhs) {
      return rhs += scalar;
    }

    /**
     * Return a new approximation resulting from elementwise
     * multiplication of the specified scalar to the mean and Cholesky
     * factor of covariance entries for the specified approximation.
     *
     * @param[in] scalar Scalar value
     * @param[in] rhs Approximation.
     * @return Multiplication of scalar by the specified approximation.
     */
    inline
    normal_fullrank operator*(double scalar, normal_fullrank rhs) {
      return rhs *= scalar;
    }

  }
}
#endif
