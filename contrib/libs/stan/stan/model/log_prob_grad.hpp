#ifndef STAN_MODEL_LOG_PROB_GRAD_HPP
#define STAN_MODEL_LOG_PROB_GRAD_HPP

#include <stan/math/rev/mat.hpp>
#include <iostream>
#include <vector>

namespace stan {
  namespace model {

    /**
     * Compute the gradient using reverse-mode automatic
     * differentiation, writing the result into the specified
     * gradient, using the specified perturbation.
     *
     * @tparam propto True if calculation is up to proportion
     * (double-only terms dropped).
     * @tparam jacobian_adjust_transform True if the log absolute
     * Jacobian determinant of inverse parameter transforms is added to
     * the log probability.
     * @tparam M Class of model.
     * @param[in] model Model.
     * @param[in] params_r Real-valued parameters.
     * @param[in] params_i Integer-valued parameters.
     * @param[out] gradient Vector into which gradient is written.
     * @param[in,out] msgs
     */
    template <bool propto, bool jacobian_adjust_transform, class M>
    double log_prob_grad(const M& model,
                         std::vector<double>& params_r,
                         std::vector<int>& params_i,
                         std::vector<double>& gradient,
                         std::ostream* msgs = 0) {
      using std::vector;
      using stan::math::var;
      double lp;
      try {
        vector<var> ad_params_r(params_r.size());
        for (size_t i = 0; i < model.num_params_r(); ++i) {
          stan::math::var var_i(params_r[i]);
          ad_params_r[i] = var_i;
        }
        var adLogProb
          = model.template log_prob<propto, jacobian_adjust_transform>
          (ad_params_r, params_i, msgs);
        lp = adLogProb.val();
        adLogProb.grad(ad_params_r, gradient);
      } catch (const std::exception &ex) {
        stan::math::recover_memory();
        throw;
      }
      stan::math::recover_memory();
      return lp;
    }

    /**
     * Compute the gradient using reverse-mode automatic
     * differentiation, writing the result into the specified
     * gradient, using the specified perturbation.
     *
     * @tparam propto True if calculation is up to proportion
     * (double-only terms dropped).
     * @tparam jacobian_adjust_transform True if the log absolute
     * Jacobian determinant of inverse parameter transforms is added to
     * the log probability.
     * @tparam M Class of model.
     * @param[in] model Model.
     * @param[in] params_r Real-valued parameters.
     * @param[out] gradient Vector into which gradient is written.
     * @param[in,out] msgs
     */
    template <bool propto, bool jacobian_adjust_transform, class M>
    double log_prob_grad(const M& model,
                         Eigen::VectorXd& params_r,
                         Eigen::VectorXd& gradient,
                         std::ostream* msgs = 0) {
      using std::vector;
      using stan::math::var;

      Eigen::Matrix<var, Eigen::Dynamic, 1> ad_params_r(params_r.size());
      for (size_t i = 0; i < model.num_params_r(); ++i) {
        stan::math::var var_i(params_r[i]);
        ad_params_r[i] = var_i;
      }
      try {
        var adLogProb
          = model
          .template log_prob<propto,
                             jacobian_adjust_transform>(ad_params_r, msgs);
        double val = adLogProb.val();
        stan::math::grad(adLogProb, ad_params_r, gradient);
        return val;
      } catch (std::exception &ex) {
        stan::math::recover_memory();
        throw;
      }
    }

  }
}
#endif
