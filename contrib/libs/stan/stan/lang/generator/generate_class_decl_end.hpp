#ifndef STAN_LANG_GENERATOR_GENERATE_CLASS_DECL_END_HPP
#define STAN_LANG_GENERATOR_GENERATE_CLASS_DECL_END_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    void generate_class_decl_end(std::ostream& o) {
      o << "}; // model" << EOL2;
    }

  }
}
#endif
