#ifndef STAN_SERVICES_DIAGNOSE_DEFAULTS_HPP
#define STAN_SERVICES_DIAGNOSE_DEFAULTS_HPP

#include <stdexcept>
#include <string>

namespace stan {
  namespace services {
    namespace diagnose {

      /**
       * Epsilon is the finite differences stepsize.
       */
      struct epsilon {
        /**
         * Return the string description of epsilon.
         *
         * @return description
         */
        static std::string description() {
          return "Finite difference stepsize.";
        }

        /**
         * Validates epsilon; epsilon must be greater than 0.
         *
         * @param[in] epsilon argument to validate
         * @throw std::invalid_argument unless epsilon is greater than zero
         */
        static void validate(double epsilon) {
          if (!(epsilon > 0))
            throw std::invalid_argument("epsilon must be greater than 0.");
        }

        /**
         * Return the default epsilon value.
         *
         * @return 1e-6
         */
        static double default_value() {
          return 1e-6;
        }
      };


      /**
       * Error is the absolute error threshold for evaluating
       * gradients relative to the finite differences calculation.
       */
      struct error {
        /**
         * Return the string description of error.
         *
         * @return description
         */
        static std::string description() {
          return "Absolute error threshold.";
        }

        /**
         * Validates error; error must be greater than 0.
         *
         * @throw std::invalid_argument unless error is greater than zero
         * equal to 0.
         */
        static void validate(double error) {
          if (!(error > 0))
            throw std::invalid_argument("error must be greater than 0.");
        }

        /**
         * Return the default error value.
         *
         * @return 1e-6
         */
        static double default_value() {
          return 1e-6;
        }
      };

    }
  }
}
#endif
