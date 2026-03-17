#ifndef STAN_LANG_GENERATOR_GENERATE_REGISTER_MPI_HPP
#define STAN_LANG_GENERATOR_GENERATE_REGISTER_MPI_HPP

#include <stan/lang/ast.hpp>
#include <string>

// REMOVE ME AFTER TESTING!!!!
#include <iostream>

namespace stan {
namespace lang {

void generate_register_mpi(const std::string& model_name,
                           std::ostream& o) {
  for (auto a : map_rect::registered_calls()) {
    int id = a.first;
    std::string fun_name = a.second;
    o << "STAN_REGISTER_MAP_RECT(" << id << ", " << model_name << "_namespace::"
      << fun_name << "_functor__"
      << ")" << std::endl;
  }
}

}
}
#endif
