#ifndef STAN_LANG_GENERATOR_GENERATE_MODEL_NAME_METHOD_HPP
#define STAN_LANG_GENERATOR_GENERATE_MODEL_NAME_METHOD_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the <code>model_name</code> method for the specified
     * name on the specified stream.
     *
     * @param[in] model_name name of model
     * @param[in,out] o stream for generating
     */
    void generate_model_name_method(const std::string& model_name,
                                    std::ostream& o) {
      o << INDENT << "static std::string model_name() {" << EOL
        << INDENT2 << "return \"" << model_name << "\";" << EOL
        << INDENT << "}" << EOL2;
    }

  }
}
#endif
