#ifndef STAN_MODEL_LOG_PROB_PROPTO_HPP
#define STAN_MODEL_LOG_PROB_PROPTO_HPP

#include <stan/math/rev/mat.hpp>
#include <iostream>
#include <vector>

namespace stan {
  namespace model {

    /**
     * Helper function to calculate log probability for
     * <code>double</code> scalars up to a proportion.
     *
     * This implementation wraps the <code>double</code> values in
     * <code>stan::math::var</code> and calls the model's
     * <code>log_prob()</code> function with <code>propto=true</code>
     * and the specified parameter for applying the Jacobian
     * adjustment for transformed parameters.
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
     * @param[in,out] msgs
     */
    template <bool jacobian_adjust_transform, class M>
    double log_prob_propto(const M& model,
                           std::vector<double>& params_r,
                           std::vector<int>& params_i,
                           std::ostream* msgs = 0) {
      using stan::math::var;
      using std::vector;
      vector<var> ad_params_r;
      ad_params_r.reserve(model.num_params_r());
      for (size_t i = 0; i < model.num_params_r(); ++i)
        ad_params_r.push_back(params_r[i]);
      try {
        double lp
          = model.template log_prob<true, jacobian_adjust_transform>
          (ad_params_r, params_i, msgs).val();
        stan::math::recover_memory();
        return lp;
      } catch (std::exception &ex) {
        stan::math::recover_memory();
        throw;
      }
    }

    /**
     * Helper function to calculate log probability for
     * <code>double</code> scalars up to a proportion.
     *
     * This implementation wraps the <code>double</code> values in
     * <code>stan::math::var</code> and calls the model's
     * <code>log_prob()</code> function with <code>propto=true</code>
     * and the specified parameter for applying the Jacobian
     * adjustment for transformed parameters.
     *
     * @tparam propto True if calculation is up to proportion
     * (double-only terms dropped).
     * @tparam jacobian_adjust_transform True if the log absolute
     * Jacobian determinant of inverse parameter transforms is added to
     * the log probability.
     * @tparam M Class of model.
     * @param[in] model Model.
     * @param[in] params_r Real-valued parameters.
     * @param[in,out] msgs
     */
    template <bool jacobian_adjust_transform, class M>
    double log_prob_propto(const M& model,
                           Eigen::VectorXd& params_r,
                           std::ostream* msgs = 0) {
      using stan::math::var;
      using std::vector;
      vector<int> params_i(0);

      double lp;
      try {
        vector<var> ad_params_r;
        ad_params_r.reserve(model.num_params_r());
        for (size_t i = 0; i < model.num_params_r(); ++i)
          ad_params_r.push_back(params_r(i));
        lp
          = model
          .template log_prob<true,
                             jacobian_adjust_transform>(ad_params_r, params_i,
                                                        msgs)
          .val();
      } catch (std::exception &ex) {
        stan::math::recover_memory();
        throw;
      }
      stan::math::recover_memory();
      return lp;
    }

  }
}
#endif
