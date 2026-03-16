#ifndef STAN_LANG_GENERATOR_GENERATE_MODEL_TYPEDEF_HPP
#define STAN_LANG_GENERATOR_GENERATE_MODEL_TYPEDEF_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate reusable typedef of <code>stan_model</code> for
     * specified model name writing to the specified stream.
     *
     * @param model_name name of model
     * @param o stream for generating
     */
    void generate_model_typedef(const std::string& model_name,
                                std::ostream& o) {
      o << "typedef " << model_name << "_namespace::" << model_name
        << " stan_model;" << EOL2;
    }

  }
}
#endif
