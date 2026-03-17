#ifndef STAN_LANG_GENERATOR_GENERATE_PARAM_NAMES_METHOD_HPP
#define STAN_LANG_GENERATOR_GENERATE_PARAM_NAMES_METHOD_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the method to <code>get_param_names</code>, which
     * retrieves the parameter names for the specified program on the
     * specified stream.
     *
     * @param[in] prog program from which to generate
     * @param[in,out] o stream for generating
     */
    void generate_param_names_method(const program& prog, std::ostream& o) {
      o << EOL << INDENT
        << "void get_param_names(std::vector<std::string>& names__) const {"
        << EOL;
      o << INDENT2 << "names__.resize(0);" << EOL;
      for (size_t i = 0; i < prog.parameter_decl_.size(); ++i)
        o << INDENT2 << "names__.push_back(\""
           << prog.parameter_decl_[i].name() << "\");" << EOL;
      for (size_t i = 0; i < prog.derived_decl_.first.size(); ++i)
        o << INDENT2 << "names__.push_back(\""
           << prog.derived_decl_.first[i].name() << "\");" << EOL;
      for (size_t i = 0; i < prog.generated_decl_.first.size(); ++i)
        o << INDENT2 << "names__.push_back(\""
          << prog.generated_decl_.first[i].name() << "\");" << EOL;
      o << INDENT << "}" << EOL2;
    }

  }
}
#endif
