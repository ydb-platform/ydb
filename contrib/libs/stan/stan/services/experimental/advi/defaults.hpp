#ifndef STAN_SERVICES_EXPERIMENTAL_ADVI_DEFAULTS_HPP
#define STAN_SERVICES_EXPERIMENTAL_ADVI_DEFAULTS_HPP

#include <stdexcept>
#include <string>

namespace stan {
  namespace services {
    namespace experimental {
      namespace advi {

        /**
         * Number of samples for Monte Carlo estimate of gradients.
         */
        struct gradient_samples {
          /**
           * Return the string description of gradient_samples.
           *
           * @return description
           */
          static std::string description() {
            return "Number of Monte Carlo draws for computing the gradient.";
          }

          /**
           * Validates gradient_samples; must be greater than 0.
           *
           * @param[in] gradient_samples argument to validate
           * @throw std::invalid_argument unless gradient_samples is greater
           *   than zero
           */
          static void validate(int gradient_samples) {
            if (!(gradient_samples > 0))
              throw std::invalid_argument("gradient_samples must be greater"
                                          " than 0.");
          }

          /**
           * Return the default number of gradient_samples.
           *
           * @return 1
           */
          static int default_value() {
            return 1;
          }
        };

        /**
         * Number of Monte Carlo samples for estimate of ELBO.
         */
        struct elbo_samples {
          /**
           * Return the string description of elbo_samples.
           *
           * @return description
           */
          static std::string description() {
            return "Number of Monte Carlo draws for estimate of ELBO.";
          }

          /**
           * Validates elbo_samples; must be greater than 0.
           *
           * @param[in] elbo_samples argument to validate
           * @throw std::invalid_argument unless elbo_samples is greater than
           *   zero
           */
          static void validate(double elbo_samples) {
            if (!(elbo_samples > 0))
              throw std::invalid_argument("elbo_samples must be greater"
                                          " than 0.");
          }

          /**
           * Return the default elbo_samples value.
           *
           * @return 100
           */
          static int default_value() {
            return 100;
          }
        };

        /**
         * Maximum number of iterations to run ADVI.
         */
        struct max_iterations {
          /**
           * Return the string description of max_iterations.
           *
           * @return description
           */
          static std::string description() {
            return "Maximum number of ADVI iterations.";
          }

          /**
           * Validates max_iterations; max_iterations must be greater than 0.
           *
           * @param[in] max_iterations argument to validate
           * @throw std::invalid_argument unless max_iterations is greater
           *    than zero
           */
          static void validate(int max_iterations) {
            if (!(max_iterations > 0))
              throw std::invalid_argument("max_iterations must be greater"
                                          " than 0.");
          }

          /**
           * Return the default max_iterations value.
           *
           * @return 10000
           */
          static int default_value() {
            return 10000;
          }
        };


        /**
         * Relative tolerance parameter for convergence.
         */
        struct tol_rel_obj {
          /**
           * Return the string description of tol_rel_obj.
           *
           * @return description
           */
          static std::string description() {
            return "Relative tolerance parameter for convergence.";
          }

          /**
           * Validates tol_rel_obj; must be greater than 0.
           *
           * @param[in] tol_rel_obj argument to validate
           * @throw std::invalid_argument unless tol_rel_obj is greater than
           *   zero
           */
          static void validate(double tol_rel_obj) {
            if (!(tol_rel_obj > 0))
              throw std::invalid_argument("tol_rel_obj must be greater"
                                          " than 0.");
          }

          /**
           * Return the default tol_rel_obj value.
           *
           * @return 0.01
           */
          static double default_value() {
            return 0.01;
          }
        };

        /**
         * Stepsize scaling parameter for variational inference
         */
        struct eta {
          /**
           * Return the string description of eta.
           *
           * @return description
           */
          static std::string description() {
            return "Stepsize scaling parameter.";
          }

          /**
           * Validates eta; must be greater than 0.
           *
           * @param[in] eta argument to validate
           * @throw std::invalid_argument unless eta is greater than zero
           */
          static void validate(double eta) {
            if (!(eta > 0))
              throw std::invalid_argument("eta must be greater than 0.");
          }

          /**
           * Return the default eta value.
           *
           * @return 1.0
           */
          static double default_value() {
            return 1.0;
          }
        };

        /**
         * Flag for eta adaptation.
         */
        struct adapt_engaged {
          /**
           * Return the string description of adapt_engaged.
           *
           * @return description
           */
          static std::string description() {
            return "Boolean flag for eta adaptation.";
          }

          /**
           * Validates adapt_engaged. This is a no-op.
           *
           * @param[in] adapt_engaged argument to validate
           */
          static void validate(bool adapt_engaged) {
          }

          /**
           * Return the default adapt_engaged value.
           *
           * @return true
           */
          static bool default_value() {
            return true;
          }
        };

        /**
         * Number of iterations for eta adaptation.
         */
        struct adapt_iterations {
          /**
           * Return the string description of adapt_iterations.
           *
           * @return description
           */
          static std::string description() {
            return "Number of iterations for eta adaptation.";
          }

          /**
           * Validates adapt_iterations; must be greater than 0.
           *
           * @param[in] adapt_iterations argument to validate
           * @throw std::invalid_argument unless adapt_iterations is
           *   greater than zero
           */
          static void validate(int adapt_iterations) {
            if (!(adapt_iterations > 0))
              throw std::invalid_argument("adapt_iterations must be greater"
                                          " than 0.");
          }

          /**
           * Return the default adapt_iterations value.
           *
           * @return 50
           */
          static int default_value() {
            return 50;
          }
        };

        /**
         * Evaluate ELBO every Nth iteration
         */
        struct eval_elbo {
          /**
           * Return the string description of eval_elbo. Evaluate
           * ELBO at every <code>eval_elbo</code> iterations.
           *
           * @return description
           */
          static std::string description() {
            return "Number of interations between ELBO evaluations";
          }

          /**
           * Validates eval_elbo; must be greater than 0.
           *
           * @param[in] eval_elbo argument to validate
           * @throw std::invalid_argument unless eval_elbo is greater than zero
           */
          static void validate(int eval_elbo) {
            if (!(eval_elbo > 0))
              throw std::invalid_argument("eval_elbo must be greater than 0.");
          }

          /**
           * Return the default eval_elbo value.
           *
           * @return 100
           */
          static int default_value() {
            return 100;
          }
        };

        /**
         * Number of approximate posterior output draws to save.
         */
        struct output_draws {
          /**
           * Return the string description of output_draws.
           *
           * @return description
           */
          static std::string description() {
            return "Number of approximate posterior output draws to save.";
          }

          /**
           * Validates output_draws; must be greater than or equal to 0.
           *
           * @param[in] output_draws argument to validate
           * @throw std::invalid_argument unless output_draws is greater than
           *   or equal to zero
           */
          static void validate(int output_draws) {
            if (!(output_draws >= 0))
              throw std::invalid_argument("output_draws must be greater than"
                                          " or equal to 0.");
          }

          /**
           * Return the default output_samples value.
           *
           * @return 1000
           */
          static int default_value() {
            return 1000;
          }
        };

      }
    }
  }
}
#endif
