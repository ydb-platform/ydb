#ifndef STAN_IO_TRIM_SPACES_HPP
#define STAN_IO_TRIM_SPACES_HPP

#include <stan/io/is_whitespace.hpp>
#include <string>

namespace stan {
namespace io {

/**
 * Return a substring of the specified string without any
 * leading or trailing spaces.
 *
 * @param x string to convert
 * @return substring of input with no leading or trailing whitespace
 */
inline std::string trim_spaces(const std::string& x) {
  std::size_t start = 0;
  while (start < x.size() && is_whitespace(x[start])) ++start;
  std::size_t end = x.size();
  while (end > 0 && is_whitespace(x[end - 1])) --end;
  return x.substr(start, end - start);
}

}
}
#endif
