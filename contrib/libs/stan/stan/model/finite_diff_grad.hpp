#ifndef STAN_MODEL_FINITE_DIFF_GRAD_HPP
#define STAN_MODEL_FINITE_DIFF_GRAD_HPP

#include <stan/callbacks/interrupt.hpp>
#include <iostream>
#include <vector>

namespace stan {
  namespace model {

    /**
     * Compute the gradient using finite differences for
     * the specified parameters, writing the result into the
     * specified gradient, using the specified perturbation.
     *
     * @tparam propto True if calculation is up to proportion
     * (double-only terms dropped).
     * @tparam jacobian_adjust_transform True if the log absolute
     * Jacobian determinant of inverse parameter transforms is added to the
     * log probability.
     * @tparam M Class of model.
     * @param model Model.
     * @param interrupt interrupt callback to be called before calculating
     *   the finite differences for each parameter.
     * @param params_r Real-valued parameters.
     * @param params_i Integer-valued parameters.
     * @param[out] grad Vector into which gradient is written.
     * @param epsilon
     * @param[in,out] msgs
     */
    template <bool propto, bool jacobian_adjust_transform, class M>
    void finite_diff_grad(const M& model,
                          stan::callbacks::interrupt& interrupt,
                          std::vector<double>& params_r,
                          std::vector<int>& params_i,
                          std::vector<double>& grad,
                          double epsilon = 1e-6,
                          std::ostream* msgs = 0) {
      std::vector<double> perturbed(params_r);
      grad.resize(params_r.size());
      for (size_t k = 0; k < params_r.size(); k++) {
        interrupt();
        perturbed[k] += epsilon;
        double logp_plus
          = model
          .template log_prob<propto,
                             jacobian_adjust_transform>(perturbed, params_i,
                                                        msgs);
        perturbed[k] = params_r[k] - epsilon;
        double logp_minus
          = model
          .template log_prob<propto,
                             jacobian_adjust_transform>(perturbed, params_i,
                                                        msgs);
        double gradest = (logp_plus - logp_minus) / (2*epsilon);
        grad[k] = gradest;
        perturbed[k] = params_r[k];
      }
    }

  }
}
#endif
