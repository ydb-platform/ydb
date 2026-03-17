#ifndef STAN_SERVICES_UTIL_EXPERIMENTAL_MESSAGE_HPP
#define STAN_SERVICES_UTIL_EXPERIMENTAL_MESSAGE_HPP

#include <stan/callbacks/logger.hpp>

namespace stan {
  namespace services {
    namespace util {

      /**
       * Writes an experimental message to the writer.
       * All experimental algorithms should call this function.
       *
       * @param[in,out] logger logger for experimental algorithm message
       */
      inline void experimental_message(stan::callbacks::logger& logger) {
        logger.info("------------------------------"
                    "------------------------------");
        logger.info("EXPERIMENTAL ALGORITHM:");
        logger.info("  This procedure has not been thoroughly tested"
                    " and may be unstable");
        logger.info("  or buggy. The interface is subject to change.");
        logger.info("------------------------------"
                    "------------------------------");
        logger.info("");
        logger.info("");
      }

    }
  }
}

#endif
