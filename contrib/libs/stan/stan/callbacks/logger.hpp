#ifndef STAN_CALLBACKS_LOGGER_HPP
#define STAN_CALLBACKS_LOGGER_HPP

#include <string>
#include <sstream>

namespace stan {
  namespace callbacks {

    /**
     * The <code>logger</code> class defines the callback
     * used by Stan's algorithms to log messages in the
     * interfaces. The base class can be used as a no-op
     * implementation.
     *
     * These are the logging levels used by <code>logger</code>,
     * in order:
     *   1. debug
     *   2. info
     *   3. warn
     *   4. error
     *   5. fatal
     */
    class logger {
    public:
      virtual ~logger() {}

      /**
       * Logs a message with debug log level
       *
       * @param[in] message message
       */
      virtual void debug(const std::string& message) { }

      /**
       * Logs a message with debug log level.
       *
       * @param[in] message message
       */
      virtual void debug(const std::stringstream& message) { }

      /**
       * Logs a message with info log level.
       *
       * @param[in] message message
       */
      virtual void info(const std::string& message) { }

      /**
       * Logs a message with info log level.
       *
       * @param[in] message message
       */
      virtual void info(const std::stringstream& message) { }

      /**
       * Logs a message with warn log level.
       *
       * @param[in] message message
       */
      virtual void warn(const std::string& message) { }

      /**
       * Logs a message with warn log level.
       *
       * @param[in] message message
       */
      virtual void warn(const std::stringstream& message) { }

      /**
       * Logs an error with error log level.
       *
       * @param[in] message message
       */
      virtual void error(const std::string& message) { }

      /**
       * Logs an error with error log level.
       *
       * @param[in] message message
       */
      virtual void error(const std::stringstream& message) { }

      /**
       * Logs an error with fatal log level.
       *
       * @param[in] message message
       */
      virtual void fatal(const std::string& message) { }

      /**
       * Logs an error with fatal log level.
       *
       * @param[in] message message
       */
      virtual void fatal(const std::stringstream& message) { }
    };
  }
}

#endif
