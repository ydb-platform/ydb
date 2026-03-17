#ifndef STAN_CALLBACKS_STREAM_LOGGER_HPP
#define STAN_CALLBACKS_STREAM_LOGGER_HPP

#include <stan/callbacks/logger.hpp>
#include <ostream>
#include <string>
#include <sstream>

namespace stan {
  namespace callbacks {

    /**
     * <code>stream_logger</code> is an implementation of
     * <code>logger</code> that writes messages to separate
     * std::stringstream outputs.
     */
    class stream_logger : public logger {
    private:
      std::ostream& debug_;
      std::ostream& info_;
      std::ostream& warn_;
      std::ostream& error_;
      std::ostream& fatal_;

    public:
      /**
       * Constructs a <code>stream_logger</code> with an output
       * stream for each log level.
       *
       * @param[in,out] debug stream to output debug messages
       * @param[in,out] info stream to output info messages
       * @param[in,out] warn stream to output warn messages
       * @param[in,out] error stream to output error messages
       * @param[in,out] fatal stream to output fatal messages
       */
      stream_logger(std::ostream& debug,
                    std::ostream& info,
                    std::ostream& warn,
                    std::ostream& error,
                    std::ostream& fatal)
        : debug_(debug), info_(info), warn_(warn), error_(error),
          fatal_(fatal) { }

      void debug(const std::string& message) {
        debug_ << message << std::endl;
      }

      void debug(const std::stringstream& message) {
        debug_ << message.str() << std::endl;
      }

      void info(const std::string& message) {
        info_ << message << std::endl;
      }

      void info(const std::stringstream& message) {
        info_ << message.str() << std::endl;
      }

      void warn(const std::string& message) {
        warn_ << message << std::endl;
      }

      void warn(const std::stringstream& message) {
        warn_ << message.str() << std::endl;
      }

      void error(const std::string& message) {
        error_ << message << std::endl;
      }

      void error(const std::stringstream& message) {
        error_ << message.str() << std::endl;
      }

      void fatal(const std::string& message) {
        fatal_ << message << std::endl;
      }

      void fatal(const std::stringstream& message) {
        fatal_ << message.str() << std::endl;
      }
    };

  }
}
#endif
