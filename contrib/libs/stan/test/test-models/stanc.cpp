#include <stan/command/stanc_helper.hpp>

#include <iostream>

/**
 * The Stan compiler (stanc).
 *
 * @param argc Number of arguments
 * @param argv Arguments
 *
 * @return 0 for success, -1 for exception, -2 for parser failure, -3
 * for invalid arguments.
 */
int main(int argc, const char* argv[]) {
  return stanc_helper(argc, argv, &std::cout, &std::cerr);
}
