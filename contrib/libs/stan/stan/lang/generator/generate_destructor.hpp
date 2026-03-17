#ifndef STAN_LANG_GENERATOR_GENERATE_DESTRUCTOR_HPP
#define STAN_LANG_GENERATOR_GENERATE_DESTRUCTOR_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the destructor for the class with name specified by
     * the model name to the specified stream.
     *
     * @param[in] model_name name of model to use as class name
     * @param[in,out] o stream for generating.
     */
    void generate_destructor(const std::string& model_name, std::ostream& o) {
      o << EOL << INDENT << "~" << model_name << "() { }"  << EOL2;
    }

  }
}
#endif
