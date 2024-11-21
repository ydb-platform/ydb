#include <string>

extern "C" size_t str_size(const std::string& str) {
  return str.size();
}
