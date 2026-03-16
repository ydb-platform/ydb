#ifndef STAN_IO_VALIDATE_ZERO_BUF_HPP
#define STAN_IO_VALIDATE_ZERO_BUF_HPP

#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <string>

namespace stan {
  namespace io {

    /**
     * Throw an bad-cast exception if the specified buffer contains
     * a digit other than 0 before an e or E.  The buffer argument
     * must implement <code>size_t size()</code> method and <code>char
     * operator[](size_t)</code>.
     *
     * @tparam B Character buffer type 
     * @throw <code>boost::bad_lexical_cast</code> if the buffer
     * contains non-zero characters before an exponentiation symbol.
     */ 
    template <typename B>
    void validate_zero_buf(const B& buf) {
      for (size_t i = 0; i < buf.size(); ++i) {
        if (buf[i] == 'e' || buf[i] == 'E')
          return;
        if (buf[i] >= '1' && buf[i] <= '9')
          boost::conversion::detail::throw_bad_cast<std::string, double>();
      }
    }

  }
}
#endif
