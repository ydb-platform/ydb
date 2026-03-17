#ifndef STAN_SERVICES_OPTIMIZE_DEFAULTS_HPP
#define STAN_SERVICES_OPTIMIZE_DEFAULTS_HPP

#include <stdexcept>
#include <string>

namespace stan {
  namespace services {
    namespace optimize {

      /**
       * Line search step size for first iteration.
       */
      struct init_alpha {
        /**
         * Return the string description of init_alpha.
         *
         * @return description
         */
        static std::string description() {
          return "Line search step size for first iteration.";
        }

        /**
         * Validates init_alpha; init_alpha must be greater than 0.
         *
         * @param[in] init_alpha argument to validate
         * @throw std::invalid_argument unless init_alpha is greater than zero
         */
        static void validate(double init_alpha) {
          if (!(init_alpha > 0))
            throw std::invalid_argument("init_alpha must be greater than 0.");
        }

        /**
         * Return the default init_alpha value.
         *
         * @return 0.001
         */
        static double default_value() {
          return 0.001;
        }
      };

      /**
       * Convergence tolerance on absolute changes in objective function value.
       */
      struct tol_obj {
        /**
         * Return the string description of tol_obj.
         *
         * @return description
         */
        static std::string description() {
          return "Convergence tolerance on absolute changes in objective"
            " function value.";
        }

        /**
         * Validates tol_obj; tol_obj must be greater than or equal to 0.
         *
         * @param[in] tol_obj argument to validate
         * @throw std::invalid_argument unless tol_obj is greater than or equal
         *   to zero
         */
        static void validate(double tol_obj) {
          if (!(tol_obj >= 0))
            throw std::invalid_argument("tol_obj must be greater"
                                        " than or equal to 0.");
        }

        /**
         * Return the default tol_obj value.
         *
         * @return 1e-12
         */
        static double default_value() {
          return 1e-12;
        }
      };

      /**
       * Convergence tolerance on relative changes in objective function value.
       */
      struct tol_rel_obj {
        /**
         * Return the string description of tol_rel_obj.
         *
         * @return description
         */
        static std::string description() {
          return "Convergence tolerance on relative changes in"
            " objective function value.";
        }

        /**
         * Validates tol_rel_obj; tol_rel_obj must be greater than or equal
         * to 0.
         *
         * @param[in] tol_rel_obj argument to validate
         * @throw std::invalid_argument unless tol_rel_obj is greater than or
         *   equal to zero
         */
        static void validate(double tol_rel_obj) {
          if (!(tol_rel_obj >= 0))
            throw std::invalid_argument("tol_rel_obj must be greater"
                                        " than or equal to 0");
        }

        /**
         * Return the default tol_rel_obj value.
         *
         * @return 10000
         */
        static double default_value() {
          return 10000;
        }
      };

      /**
       * Convergence tolerance on the norm of the gradient.
       */
      struct tol_grad {
        /**
         * Return the string description of tol_grad.
         *
         * @return description
         */
        static std::string description() {
          return "Convergence tolerance on the norm of the gradient.";
        }

        /**
         * Validates tol_grad; tol_grad must be greater than or equal to 0.
         *
         * @param[in] tol_grad argument to validate
         * @throw std::invalid_argument unless tol_grad is greater than or
         *   equal to zero
         */
        static void validate(double tol_grad) {
          if (!(tol_grad >= 0))
            throw std::invalid_argument("tol_grad must be greater"
                                        " than or equal to 0");
        }

        /**
         * Return the default tol_grad value.
         *
         * @return 1e-8
         */
        static double default_value() {
          return 1e-8;
        }
      };


      /**
       * Convergence tolerance on the relative norm of the gradient.
       */
      struct tol_rel_grad {
        /**
         * Return the string description of tol_rel_grad.
         *
         * @return description
         */
        static std::string description() {
          return "Convergence tolerance on the relative norm of the gradient.";
        }

        /**
         * Validates tol_rel_grad; tol_rel_grad must be greater than
         * or equal to 0.
         *
         * @param[in] tol_rel_grad argument to validate
         * @throw std::invalid_argument unless tol_rel_grad is greater than or
         *   equal to zero
         */
        static void validate(double tol_rel_grad) {
          if (!(tol_rel_grad >= 0))
            throw std::invalid_argument("tol_rel_grad must be greater"
                                        " than or equal to 0.");
        }

        /**
         * Return the default tol_rel_grad value.
         *
         * @return 10000000
         */
        static double default_value() {
          return 10000000;
        }
      };

      /**
       * Convergence tolerance on changes in parameter value.
       */
      struct tol_param {
        /**
         * Return the string description of tol_param.
         *
         * @return description
         */
        static std::string description() {
          return "Convergence tolerance on changes in parameter value.";
        }

        /**
         * Validates tol_param; tol_param must be greater than or equal to 0.
         *
         * @param[in] tol_param argument to validate
         * @throw std::invalid_argument unless tol_param is greater than or
         *    equal to zero
         */
        static void validate(double tol_param) {
          if (!(tol_param >= 0))
            throw std::invalid_argument("tol_param");
        }

        /**
         * Return the default tol_param.
         *
         * @return 1e-08
         */
        static double default_value() {
          return 1e-08;
        }
      };

      /**
       * Amount of history to keep for L-BFGS.
       */
      struct history_size {
        /**
         * Return the string description of history_size.
         *
         * @return description
         */
        static std::string description() {
          return "Amount of history to keep for L-BFGS.";
        }

        /**
         * Validates history_size; history_size must be greater than 0.
         *
         * @param[in] history_size argument to validate
         * @throw std::invalid_argument unless history_size is greater than
         *   zero
         */
        static void validate(int history_size) {
          if (!(history_size > 0))
            throw std::invalid_argument("history_size must be greater than 0.");
        }

        /**
         * Return the default history_size value.
         *
         * @return 5
         */
        static int default_value() {
          return 5;
        }
      };

      /**
       * Total number of iterations.
       */
      struct iter {
        /**
         * Return the string description of iter.
         *
         * @return description
         */
        static std::string description() {
          return "Total number of iterations.";
        }

        /**
         * Validates iter; iter must be greater than 0.
         *
         * @param[in] iter argument to validate
         * @throw std::invalid_argument unless iter is greater than zero
         */
        static void validate(int iter) {
          if (!(iter > 0))
            throw std::invalid_argument("iter must be greater than 0.");
        }

        /**
         * Return the default iter value.
         *
         * @return 2000
         */
        static int default_value() {
          return 2000;
        }
      };

      /**
       * Save optimization interations to output.
       */
      struct save_iterations {
        /**
         * Return the string description of save_iterations.
         *
         * @return description
         */
        static std::string description() {
          return "Save optimization interations to output.";
        }

        /**
         * Validates save_iterations. This is a no-op.
         *
         * @param[in] save_iterations argument to validate
         */
        static void validate(bool save_iterations) {
        }

        /**
         * Return the default save_iterations value.
         *
         * @return false
         */
        static bool default_value() {
          return false;
        }
      };

    }
  }
}
#endif
