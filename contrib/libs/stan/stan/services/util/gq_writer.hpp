#ifndef STAN_SERVICES_UTIL_GQ_WRITER_HPP
#define STAN_SERVICES_UTIL_GQ_WRITER_HPP

#include <stan/callbacks/logger.hpp>
#include <stan/callbacks/writer.hpp>
#include <stan/mcmc/base_mcmc.hpp>
#include <stan/mcmc/sample.hpp>
#include <stan/model/prob_grad.hpp>
#include <sstream>
#include <iomanip>
#include <string>
#include <vector>

namespace stan {
  namespace services {
    namespace util {

      /**
       * gq_writer writes out
       *
       * @tparam Model Model class
       */
      class gq_writer {
      private:
        callbacks::writer& sample_writer_;
        callbacks::logger& logger_;
        int num_constrained_params_;

      public:
        /**
         * Constructor.
         *
         * @param[in,out] sample_writer samples are "written" to this stream
         * @param[in,out] logger messages are written through the logger
         * @param[in] num_constrained_params offset into write_array gqs
         */
        gq_writer(callbacks::writer& sample_writer, callbacks::logger& logger,
                  int num_constrained_params): sample_writer_(sample_writer),
                  logger_(logger),
                  num_constrained_params_(num_constrained_params) { }

        /**
         * Write names of variables declared in the generated quantities block
         * to stream `sample_writer_`.
         *
         * @tparam M model class
         */
        template <class Model>
        void write_gq_names(const Model& model) {
          static const bool include_tparams = false;
          static const bool include_gqs = true;
          std::vector<std::string> names;
          model.constrained_param_names(names, include_tparams, include_gqs);
          std::vector<std::string> gq_names(names.begin()
                                            + num_constrained_params_,
                                            names.end());
          sample_writer_(gq_names);
        }

        /**
         * Calls model's `write_array` method and writes values of
         * variables defined in the generated quantities block
         * to stream `sample_writer_`.
         *
         * @tparam M model class
         * @tparam RNG pseudo random number generator class
         * @param[in] model instantiated model
         * @param[in] rng instantiated RNG
         * @param[in] draw sequence unconstrained parameters values.
         */
        template <class Model, class RNG>
        void write_gq_values(const Model& model,
                             RNG& rng,
                             const std::vector<double>& draw) {
          std::vector<double> values;
          std::vector<int> params_i;  // unused - no discrete params
          std::stringstream ss;
          try {
            model.write_array(rng,
                              const_cast<std::vector<double>&>(draw),
                              params_i,
                              values,
                              false,
                              true,
                              &ss);
          } catch (const std::exception& e) {
            if (ss.str().length() > 0)
              logger_.info(ss);
            logger_.info(e.what());
            return;
          }
          if (ss.str().length() > 0)
            logger_.info(ss);

          std::vector<double> gq_values(values.begin()
                                        + num_constrained_params_,
                                        values.end());
          sample_writer_(gq_values);
        }
      };

    }
  }
}
#endif
