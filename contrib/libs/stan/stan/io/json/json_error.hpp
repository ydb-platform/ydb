#ifndef STAN_IO_JSON_JSON_ERROR_HPP
#define STAN_IO_JSON_JSON_ERROR_HPP

#include <stdexcept>
#include <string>

namespace stan {

  namespace json {

    /**
     * Exception type for JSON errors.
     */
    struct json_error : public std::logic_error {
      /**
       * Construct a JSON error with the specified message
       * @param what Message to attach to error
       */
      explicit json_error(const std::string& what)
        : logic_error(what) {
      }
    };

  }
}
#endif
