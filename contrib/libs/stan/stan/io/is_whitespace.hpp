#ifndef STAN_IO_IS_WHITESPACE_HPP
#define STAN_IO_IS_WHITESPACE_HPP

#include <string>

namespace stan {
namespace io {

/**
 * Returns true if the specified character is an ASCII space character.
 * The space characters are the space, newline, carriage return, and tab.
 *
 * @param c character to test
 * @return true if it is an ASCII whitespace character
 */
inline bool is_whitespace(char c) {
  return c == ' ' || c == '\n' || c == '\r' || c == '\t';
}

}
}
#endif
