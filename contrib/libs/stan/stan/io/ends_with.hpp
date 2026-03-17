#ifndef STAN_IO_ENDS_WITH_HPP
#define STAN_IO_ENDS_WITH_HPP

#include <string>

namespace stan {
namespace io {

/**
 * Return true if the specified string starts with the specified
 * prefix.
 *
 * @param p prefix
 * @param s string to test
 * @return true if s has p as a prefix
 */
inline bool ends_with(const std::string& p, const std::string& s) {
  return s.size() >= p.size() && s.substr(s.size() - p.size()) == p;
}

}
}
#endif
