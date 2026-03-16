#ifndef STAN_SERVICES_SAMPLE_DEFAULTS_HPP
#define STAN_SERVICES_SAMPLE_DEFAULTS_HPP

#include <stdexcept>
#include <string>

namespace stan {
  namespace services {
    namespace sample {

      /**
       * Number of sampling iterations.
       */
      struct num_samples {
        /**
         * Return the string description of num_samples.
         *
         * @return description
         */
        static std::string description() {
          return "Number of sampling iterations.";
        }

        /**
         * Validates num_samples; num_samples must be greater than or
         * equal to 0.
         *
         * @param[in] num_samples argument to validate
         * @throw std::invalid_argument unless num_samples is greater
         *   than or equal to zero
         */
        static void validate(int num_samples) {
          if (!(num_samples >= 0))
            throw std::invalid_argument("num_samples must be greater"
                                        " than or equal to 0.");
        }

        /**
         * Return the default num_samples value.
         *
         * @return 1000
         */
        static int default_value() {
          return 1000;
        }
      };

      /**
       * Number of warmup iterations.
       */
      struct num_warmup {
        /**
         * Return the string description of num_warmup.
         *
         * @return description
         */
        static std::string description() {
          return "Number of warmup iterations.";
        }

        /**
         * Validates num_warmup; num_warmup must be greater than or
         * equal to 0.
         *
         * @param[in] num_warmup argument to validate
         * @throw std::invalid_argument unless num_warmup is greater than
         *   or equal to zero
         */
        static void validate(int num_warmup) {
          if (!(num_warmup >= 0))
            throw std::invalid_argument("num_warmup must be greater"
                                        " than or equal to 0.");
        }

        /**
         * Return the default num_warmup value.
         *
         * @return 1000
         */
        static int default_value() {
          return 1000;
        }
      };

      /**
       * Save warmup iterations to output.
       */
      struct save_warmup {
        /**
         * Return the string description of save_warmup.
         *
         * @return description
         */
        static std::string description() {
          return "Save warmup iterations to output.";
        }

        /**
         * Validates save_warmup. This is a no-op.
         *
         * @param[in] save_warmup argument to validate
         */
        static void validate(bool save_warmup) {
        }

        /**
         * Return the default save_warmup value.
         *
         * @return false
         */
        static bool default_value() {
          return false;
        }
      };

      /**
       * Period between saved samples.
       */
      struct thin {
        /**
         * Return the string description of thin.
         *
         * @return description
         */
        static std::string description() {
          return "Period between saved samples.";
        }

        /**
         * Validates thin; thin must be greater than 0.
         *
         * @param[in] thin argument to validate
         * @throw std::invalid_argument unless thin is greater than zero
         */
        static void validate(int thin) {
          if (!(thin > 0))
            throw std::invalid_argument("thin must be greater than 0.");
        }

        /**
         * Return the default thin value.
         *
         * @return 1
         */
        static int default_value() {
          return 1;
        }
      };

      /**
       * Indicates whether adaptation is engaged.
       */
      struct adaptation_engaged {
        /**
         * Return the string description of adaptation_engaged.
         *
         * @return description
         */
        static std::string description() {
          return "Indicates whether adaptation is engaged.";
        }

        /**
         * Validates adaptation_engaged. This is a no op.
         *
         * @param[in] adaptation_engaged argument to validate
         */
        static void validate(bool adaptation_engaged) {
        }

        /**
         * Return the default adaptation_engaged value.
         *
         * @return true
         */
        static bool default_value() {
          return true;
        }
      };


      /**
       * Adaptation regularization scale.
       */
      struct gamma {
        /**
         * Return the string description of gamma.
         *
         * @return description
         */
        static std::string description() {
          return "Adaptation regularization scale.";
        }

        /**
         * Validates gamma; gamma must be greater than 0.
         *
         * @param[in] gamma argument to validate
         * @throw std::invalid_argument unless gamma is greater than zero
         */
        static void validate(double gamma) {
          if (!(gamma > 0))
            throw std::invalid_argument("gamma must be greater than 0.");
        }

        /**
         * Return the default gamma value.
         *
         * @return 0.05
         */
        static double default_value() {
          return 0.05;
        }
      };

      /**
       * Adaptation relaxation exponent.
       */
      struct kappa {
        /**
         * Return the string description of kappa.
         *
         * @return description
         */
        static std::string description() {
          return "Adaptation relaxation exponent.";
        }

        /**
         * Validates kappa; kappa must be greater than 0.
         *
         * @param[in] kappa argument to validate
         * @throw std::invalid_argument unless kappa is greater than zero
         */
        static void validate(double kappa) {
          if (!(kappa > 0))
            throw std::invalid_argument("kappa must be greater than 0.");
        }

        /**
         * Return the default kappa value.
         *
         * @return 0.75
         */
        static double default_value() {
          return 0.75;
        }
      };

      /**
       * Adaptation iteration offset.
       */
      struct t0 {
        /**
         * Return the description of t0.
         *
         * @return description
         */
        static std::string description() {
          return "Adaptation iteration offset.";
        }

        /**
         * Validates t0; t0 must be greater than 0.
         *
         * @param[in] t0 argument to validate
         * @throw std::invalid_argument unless t0 is greater than zero
         */
        static void validate(double t0) {
          if (!(t0 > 0))
            throw std::invalid_argument("t0 must be greater than 0.");
        }

        /**
         * Return the default t0 value.
         *
         * @return 10
         */
        static double default_value() {
          return 10;
        }
      };

      /**
       * Width of initial fast adaptation interval.
       */
      struct init_buffer {
        /**
         * Return the string description of init_buffer.
         *
         * @return description
         */
        static std::string description() {
          return "Width of initial fast adaptation interval.";
        }

        /**
         * Validates init_buffer. This is a no op.
         *
         * @param[in] init_buffer argument to validate
         */
        static void validate(unsigned int init_buffer) {
        }

        /**
         * Return the default init_buffer value.
         *
         * @return 75
         */
        static unsigned int default_value() {
          return 75;
        }
      };

      /**
       * Width of final fast adaptation interval.
       */
      struct term_buffer {
        /**
         * Return the string description of term_buffer.
         *
         * @return description
         */
        static std::string description() {
          return "Width of final fast adaptation interval.";
        }

        /**
         * Validates term_buffer. This is a no-op.
         *
         * @param[in] term_buffer argument to validate
         */
        static void validate(unsigned int term_buffer) {
        }

        /**
         * Return the default term_buffer value.
         *
         * @return 50
         */
        static unsigned int default_value() {
          return 50;
        }
      };

      /**
       * Initial width of slow adaptation interval.
       */
      struct window {
        /**
         * Return the string description of window.
         *
         * @return description
         */
        static std::string description() {
          return "Initial width of slow adaptation interval.";
        }

        /**
         * Validates window. This is a no op.
         *
         * @param[in] window argument to validate
         */
        static void validate(unsigned int window) {
        }

        /**
         * Return the default window value.
         *
         * @return 25
         */
        static unsigned int default_value() {
          return 25;
        }
      };

      /**
       * Total integration time for Hamiltonian evolution.
       */
      struct int_time {
        /**
         * Return the string description of int_time.
         *
         * @return description
         */
        static std::string description() {
          return "Total integration time for Hamiltonian evolution.";
        }

        /**
         * Validates int_time. int_time must be greater than 0.
         *
         * @param[in] int_time argument to validate
         * @throw std::invalid_argument unless int_time is greater than zero
         */
        static void validate(double int_time) {
          if (!(int_time > 0))
            throw std::invalid_argument("int_time must be greater than 0.");
        }

        /**
         * Return the default int_time value.
         *
         * @return 2 * pi
         */
        static double default_value() {
          return 6.28318530717959;
        }
      };

      /**
       * Maximum tree depth.
       */
      struct max_depth {
        /**
         * Return the string description of max_depth.
         *
         * @return description
         */
        static std::string description() {
          return "Maximum tree depth.";
        }

        /**
         * Validates max_depth; max_depth must be greater than 0.
         *
         * @param[in] max_depth argument to validate
         * @throw std::invalid_argument unless max_depth is greater than zero
         */
        static void validate(int max_depth) {
          if (!(max_depth > 0))
            throw std::invalid_argument("max_depth must be greater than 0.");
        }

        /**
         * Return the default max_depth value.
         *
         * @return 10
         */
        static int default_value() {
          return 10;
        }
      };

      /**
       * Step size for discrete evolution
       */
      struct stepsize {
        /**
         * Return the string description of stepsize.
         *
         * @return description
         */
        static std::string description() {
          return "Step size for discrete evolution.";
        }

        /**
         * Validates stepsize; stepsize must be greater than 0.
         *
         * @param[in] stepsize argument to validate
         * @throw std::invalid_argument unless stepsize is greater than zero
         */
        static void validate(double stepsize) {
          if (!(stepsize > 0))
            throw std::invalid_argument("stepsize must be greater than 0.");
        }

        /**
         * Return the default stepsize value.
         *
         * @return 1
         */
        static double default_value() {
          return 1;
        }
      };

      /**
       * Uniformly random jitter of the stepsize, in percent.
       */
      struct stepsize_jitter {
        /**
         * Return the string description of stepsize_jitter.
         *
         * @return description
         */
        static std::string description() {
          return "Uniformly random jitter of the stepsize, in percent.";
        }

        /**
         * Validates stepsize_jitter; stepsize_jitter must be between 0 and 1.
         *
         * @param[in] stepsize_jitter argument to validate
         * @throw std::invalid_argument unless stepsize_jitter is between 0 and
         *   1, inclusive
         */
        static void validate(double stepsize_jitter) {
          if (!(stepsize_jitter >= 0 && stepsize_jitter <= 1))
            throw std::invalid_argument("stepsize_jitter must be between"
                                        " 0 and 1.");
        }

        /**
         * Return the default stepsize_jitter value.
         *
         * @return 0
         */
        static double default_value() {
          return 0;
        }
      };

    }
  }
}
#endif
