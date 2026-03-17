#ifndef STAN_MODEL_GRAD_HESS_LOG_PROB_HPP
#define STAN_MODEL_GRAD_HESS_LOG_PROB_HPP

#include <stan/model/log_prob_grad.hpp>
#include <iostream>
#include <vector>

namespace stan {
  namespace model {

    /**
     * Evaluate the log-probability, its gradient, and its Hessian
     * at params_r. This default version computes the Hessian
     * numerically by finite-differencing the gradient, at a cost of
     * O(params_r.size()^2).
     *
     * @tparam propto True if calculation is up to proportion
     * (double-only terms dropped).
     * @tparam jacobian_adjust_transform True if the log absolute
     * Jacobian determinant of inverse parameter transforms is added to the
     * log probability.
     * @tparam M Class of model.
     * @param[in] model Model.
     * @param[in] params_r Real-valued parameter vector.
     * @param[in] params_i Integer-valued parameter vector.
     * @param[out] gradient Vector to write gradient to.
     * @param[out] hessian Vector to write gradient to. hessian[i*D + j]
     * gives the element at the ith row and jth column of the Hessian
     * (where D=params_r.size()).
     * @param[in, out] msgs Stream to which print statements in Stan
     * programs are written, default is 0
     */
    template <bool propto, bool jacobian_adjust_transform, class M>
    double grad_hess_log_prob(const M& model, std::vector<double>& params_r,
                              std::vector<int>& params_i,
                              std::vector<double>& gradient,
                              std::vector<double>& hessian,
                              std::ostream* msgs = 0) {
      static const double epsilon = 1e-3;
      static const double half_epsilon = 0.5 * epsilon;
      static const int order = 4;
      static const double perturbations[order]
        = {-2*epsilon, -1*epsilon, epsilon, 2*epsilon};
      static const double coefficients[order]
        = { 1.0 / 12.0, -2.0 / 3.0, 2.0 / 3.0, -1.0 / 12.0 };

      double result
        = log_prob_grad<propto, jacobian_adjust_transform>(model, params_r,
                                                           params_i, gradient,
                                                           msgs);
      hessian.assign(params_r.size() * params_r.size(), 0);
      std::vector<double> temp_grad(params_r.size());
      std::vector<double> perturbed_params(params_r.begin(), params_r.end());
      for (size_t d = 0; d < params_r.size(); ++d) {
        double* row = &hessian[d*params_r.size()];
        for (int i = 0; i < order; ++i) {
          perturbed_params[d] = params_r[d] + perturbations[i];
          log_prob_grad<propto, jacobian_adjust_transform>(model,
                                                           perturbed_params,
                                                           params_i, temp_grad);
          for (size_t dd = 0; dd < params_r.size(); ++dd) {
            double increment = half_epsilon * coefficients[i] * temp_grad[dd];
            row[dd] += increment;
            hessian[d + dd*params_r.size()] += increment;
          }
        }
        perturbed_params[d] = params_r[d];
      }
      return result;
    }

  }
}
#endif
