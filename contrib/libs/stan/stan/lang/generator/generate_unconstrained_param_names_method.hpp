#ifndef STAN_LANG_GENERATOR_GENERATE_UNCONSTRAINED_PARAM_NAMES_METHOD_HPP
#define STAN_LANG_GENERATOR_GENERATE_UNCONSTRAINED_PARAM_NAMES_METHOD_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_unconstrained_param_names_array.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the method <code>unconstrained_param_names</code> for
     * the specified program on the specified stream.
     *
     * @param[in] prog progam from which to generate
     * @param[in,out] o stream for generating
     */
    void generate_unconstrained_param_names_method(const program& prog,
                                                   std::ostream& o) {
      o << EOL << INDENT
        << "void unconstrained_param_names("
        << "std::vector<std::string>& param_names__,"
        << EOL << INDENT
        << "                               bool include_tparams__ = true,"
        << EOL << INDENT
        << "                               bool include_gqs__ = true) const {"
        << EOL << INDENT2
        << "std::stringstream param_name_stream__;" << EOL;
      for (size_t i = 0; i < prog.parameter_decl_.size(); ++i)
        generate_unconstrained_param_names_array(2, o, prog.parameter_decl_[i]);
      o << EOL << INDENT2
        << "if (!include_gqs__ && !include_tparams__) return;"  << EOL;

      o << EOL << INDENT2 << "if (include_tparams__) {"  << EOL;
      for (size_t i = 0; i < prog.derived_decl_.first.size(); ++i)
        generate_unconstrained_param_names_array(3, o,
                                                 prog.derived_decl_.first[i]);
      o << INDENT2 << "}" << EOL;

      o << EOL << INDENT2 << "if (!include_gqs__) return;" << EOL;
      for (size_t i = 0; i < prog.generated_decl_.first.size(); ++i)
        generate_unconstrained_param_names_array(2, o,
                                                 prog.generated_decl_.first[i]);
      o << INDENT << "}" << EOL2;
    }

  }
}
#endif
