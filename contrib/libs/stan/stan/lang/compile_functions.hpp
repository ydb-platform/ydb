#ifndef STAN_LANG_COMPILE_FUNCTIONS_HPP
#define STAN_LANG_COMPILE_FUNCTIONS_HPP

#include <stan/lang/generator/generate_standalone_functions.hpp>
#include <stan/lang/parser.hpp>

#include <iostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Read a Stan file with only the functions block from the
     * specified input, parse it, and write the C++ code for it
     * to the specified output.
     *
     * @param[in] msgs Output stream for warning messages
     * @param[in] stan_funcs_in Stan model specification
     * @param[in] cpp_out C++ code output stream
     * @param[in] namespaces Vector of namespace to generate the functions in
     * @param[in] allow_undefined Permit undefined functions?
     * @param filename name of file or other source from which input
     *   stream was derived
     * @param include_paths array of paths to search for included files
     * @return <code>false</code> if code could not be generated
     *    due to syntax error in the functions file;
     *    <code>true</code> otherwise.
     */
    bool compile_functions(std::ostream* msgs,
                           std::istream& stan_funcs_in,
                           std::ostream& cpp_out,
                           const std::vector<std::string>& namespaces,
                           const bool allow_undefined = false,
                           const std::string& filename = "unknown file name",
                           const std::vector<std::string>& include_paths
                           = std::vector<std::string>()) {
      io::program_reader reader(stan_funcs_in, filename, include_paths);
      std::string s = reader.program();
      std::stringstream ss(s);
      program prog;

      std::string name("functions_only_model");

      bool parsed_ok = parse(msgs, ss, name, reader, prog, allow_undefined);
      if (!parsed_ok)
        return false;  // syntax error in program

      generate_standalone_functions(prog, namespaces,
                                    reader.history(), cpp_out);
      return true;
    }


  }
}
#endif
