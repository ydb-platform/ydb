#ifndef STAN_CALLBACKS_WRITER_HPP
#define STAN_CALLBACKS_WRITER_HPP

#include <boost/lexical_cast.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace callbacks {

    /**
     * <code>writer</code> is a base class defining the interface
     * for Stan writer callbacks. The base class can be used as a
     * no-op implementation.
     */
    class writer {
    public:
      /**
       * Virtual destructor.
       */
      virtual ~writer() {}

      /**
       * Writes a set of names.
       *
       * @param[in] names Names in a std::vector
       */
      virtual void operator()(const std::vector<std::string>& names) {
      }

      /**
       * Writes a set of values.
       *
       * @param[in] state Values in a std::vector
       */
      virtual void operator()(const std::vector<double>& state) {
      }

      /**
       * Writes blank input.
       */
      virtual void operator()() {
      }

      /**
       * Writes a string.
       *
       * @param[in] message A string
       */
      virtual void operator()(const std::string& message) {
      }
    };

  }
}
#endif
