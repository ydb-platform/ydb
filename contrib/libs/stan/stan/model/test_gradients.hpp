#ifndef STAN_MODEL_TEST_GRADIENTS_HPP
#define STAN_MODEL_TEST_GRADIENTS_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/model/finite_diff_grad.hpp>
#include <stan/model/log_prob_grad.hpp>
#include <cmath>
#include <sstream>
#include <vector>

namespace stan {
  namespace model {

    /**
     * Test the log_prob_grad() function's ability to produce
     * accurate gradients using finite differences.  This shouldn't
     * be necessary when using autodiff, but is useful for finding
     * bugs in hand-written code (or var).
     *
     * @tparam propto True if calculation is up to proportion
     * (double-only terms dropped).
     * @tparam jacobian_adjust_transform True if the log absolute
     * Jacobian determinant of inverse parameter transforms is added to the
     * log probability.
     * @tparam Model Class of model.
     * @param[in] model Model.
     * @param[in] params_r Real-valued parameter vector.
     * @param[in] params_i Integer-valued parameter vector.
     * @param[in] epsilon Real-valued scalar saying how much to perturb.
     *   Reasonable value is 1e-6.
     * @param[in] error Real-valued scalar saying how much error to allow.
     *   Reasonable value is 1e-6.
     * @param[in,out] interrupt callback to be called at every iteration
     * @param[in,out] logger Logger for messages
     * @param[in,out] parameter_writer Writer callback for file output
     * @return number of failed gradient comparisons versus allowed
     * error, so 0 if all gradients pass
     */
    template <bool propto, bool jacobian_adjust_transform, class Model>
    int test_gradients(const Model& model,
                       std::vector<double>& params_r,
                       std::vector<int>& params_i,
                       double epsilon,
                       double error,
                       stan::callbacks::interrupt& interrupt,
                       stan::callbacks::logger& logger,
                       stan::callbacks::writer& parameter_writer) {
      std::stringstream msg;
      std::vector<double> grad;
      double lp = log_prob_grad<propto, jacobian_adjust_transform>(model,
                                                                   params_r,
                                                                   params_i,
                                                                   grad,
                                                                   &msg);
      if (msg.str().length() > 0) {
        logger.info(msg);
        parameter_writer(msg.str());
      }

      std::vector<double> grad_fd;
      finite_diff_grad<false, true, Model>(model, interrupt, params_r, params_i,
                                           grad_fd, epsilon, &msg);
      if (msg.str().length() > 0) {
        logger.info(msg);
        parameter_writer(msg.str());
      }

      int num_failed = 0;

      std::stringstream lp_msg;
      lp_msg << " Log probability=" << lp;

      parameter_writer();
      parameter_writer(lp_msg.str());
      parameter_writer();

      logger.info("");
      logger.info(lp_msg);
      logger.info("");

      std::stringstream header;
      header << std::setw(10) << "param idx"
             << std::setw(16) << "value"
             << std::setw(16) << "model"
             << std::setw(16) << "finite diff"
             << std::setw(16) << "error";

      parameter_writer(header.str());
      logger.info(header);

      for (size_t k = 0; k < params_r.size(); k++) {
        std::stringstream line;
        line << std::setw(10) << k
             << std::setw(16) << params_r[k]
             << std::setw(16) << grad[k]
             << std::setw(16) << grad_fd[k]
             << std::setw(16) << (grad[k] - grad_fd[k]);
        parameter_writer(line.str());
        logger.info(line);
        if (std::fabs(grad[k] - grad_fd[k]) > error)
          num_failed++;
      }
      return num_failed;
    }

  }
}
#endif
