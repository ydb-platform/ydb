#ifndef STAN_IO_READ_LINE_HPP
#define STAN_IO_READ_LINE_HPP

#include <istream>
#include <sstream>
#include <string>

namespace stan {
  namespace io {

    /**
     * Returns the next line read from the specified stream, or the
     * empty string if the stream is empty.
     *
     * <p>The input stream is read character by character and not
     * buffered, though the use cases are only for small files of
     * code, so this probably isn't a big deal.
     *
     * @param in input stream
     * @return next line from stream or empty string if empty
     */
    inline std::string read_line(std::istream& in) {
      std::stringstream ss;
      while (true) {
        int c = in.get();
        if (c == std::char_traits<char>::eof())
          return ss.str();
        ss << static_cast<char>(c);
        if (c == '\n')
          return ss.str();
      }
    }

  }
}
#endif
