#ifndef STAN_LANG_GENERATOR_GENERATE_STANDALONE_FUNCTIONS_HPP
#define STAN_LANG_GENERATOR_GENERATE_STANDALONE_FUNCTIONS_HPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/generator/generate_functions.hpp>
#include <stan/lang/generator/generate_function_instantiations.hpp>
#include <stan/lang/generator/generate_includes.hpp>
#include <stan/lang/generator/generate_program_reader_fun.hpp>
#include <stan/lang/generator/generate_typedefs.hpp>
#include <stan/lang/generator/generate_usings_standalone_functions.hpp>
#include <stan/lang/generator/generate_version_comment.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the C++ code for standalone functions, generating it
     * in the namespace provided,
     * writing to the specified stream.
     *
     * @param[in] prog program from which to generate
     * @param[in] namespaces namespace to generate the functions in
     * @param[in] history I/O include history for text underlying
     *   program
     * @param[in,out] o stream for generating
     */
    void generate_standalone_functions(
           const program& prog,
           const std::vector<std::string>& namespaces,
           const std::vector<io::preproc_event>& history,
           std::ostream& o) {
      generate_version_comment(o);

      generate_include("stan/model/standalone_functions_header.hpp", o);
      o << EOL;

      // generate namespace starts
      for (size_t namespace_i = 0;
          namespace_i < namespaces.size(); ++namespace_i) {
        o << "namespace " << namespaces[namespace_i] << " { ";
      }
      o << EOL;

      generate_usings_standalone_functions(o);
      generate_typedefs(o);
      generate_program_reader_fun(history, o);
      generate_functions(prog.function_decl_defs_, o);

      // generate namespace ends
      for (size_t namespace_i = 0;
          namespace_i < namespaces.size(); ++namespace_i) {
        o << " } ";
      }
      o << EOL;

      generate_function_instantiations(prog.function_decl_defs_, namespaces, o);
    }

  }
}
#endif
