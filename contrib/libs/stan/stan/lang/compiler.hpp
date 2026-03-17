#ifndef STAN_LANG_COMPILER_HPP
#define STAN_LANG_COMPILER_HPP

#include <stan/io/program_reader.hpp>
#include <stan/lang/ast.hpp>
#include <stan/lang/generator.hpp>
#include <stan/lang/parser.hpp>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Read a Stan model specification from the specified input, parse
     * it, and write the C++ code for it to the specified output,
     * allowing undefined function declarations if the flag is set to
     * true and searching the specified include path for included
     * files.
     *
     * @param msgs Output stream for warning messages
     * @param in Stan model specification
     * @param out C++ code output stream
     * @param name Name of model class
     * @param allow_undefined true if permits undefined functions
     * @param filename name of file or other source from which input
     *   stream was derived
     * @param include_paths array of paths to search for included files
     * @return <code>false</code> if code could not be generated due
     *   to syntax error in the Stan model; <code>true</code>
     *   otherwise.
     */
    bool compile(std::ostream* msgs, std::istream& in, std::ostream& out,
                 const std::string& name, const bool allow_undefined = false,
                 const std::string& filename = "unknown file name",
                 const std::vector<std::string>& include_paths
                  = std::vector<std::string>()) {
      io::program_reader reader(in, filename, include_paths);
      std::string s = reader.program();
      std::stringstream ss(s);
      program prog;
      bool parse_succeeded = parse(msgs, ss, name, reader, prog,
                                   allow_undefined);
      if (!parse_succeeded)
        return false;
      generate_cpp(prog, name, reader.history(), out);
      return true;
    }

  }
}
#endif
