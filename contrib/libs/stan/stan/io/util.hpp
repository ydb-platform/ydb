#ifndef STAN_IO_UTIL_HPP
#define STAN_IO_UTIL_HPP

#include <string>
#include <ctime>

namespace stan {

  namespace io {

    /**
     * Return the current coordinated universal time (UTC) as a string.
     *
     * Output is of the form "Fri Feb 24 21:15:36 2012"
     *
     * @return String representation of current UTC.
     */
    std::string utc_time_string() {
      // FIXME:  use std::strftime

      // original with asctime
      // std::time_t rawtime = time(0);
      // std::tm *time = gmtime(&rawtime);
      // return std::string(asctime(time));

      // new with strfitime
      time_t rawtime;
      std::time(&rawtime);

      char cbuf[80];
      std::strftime(cbuf, 80, "%a %b %d %Y %H:%M:%S",
                    std::localtime(&rawtime));

      return std::string(cbuf);
    }

  }
}

#endif
