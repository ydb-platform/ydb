#ifndef STAN_IO_STARTS_WITH_HPP
#define STAN_IO_STARTS_WITH_HPP

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
inline bool starts_with(const std::string& p,
                        const std::string& s) {
  return s.size() >= p.size() && s.substr(0, p.size()) == p;
}

}
}
#endif
