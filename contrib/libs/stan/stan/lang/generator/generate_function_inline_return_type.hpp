#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_INLINE_RETURN_TYPE_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_INLINE_RETURN_TYPE_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_bare_type.hpp>
#include <stan/lang/generator/generate_indent.hpp>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the return type for the specified function declaration
     * in the context of the specified scalar type at the specified
     * indentation level on the specified stream.
     *
     * @param[in] fun function declaration
     * @param[in] scalar_t_name string version of scalar type in
     * context
     * @param[in] indent indentation level
     * @param[in,out] out stream for generating
     */
    void generate_function_inline_return_type(const function_decl_def& fun,
                                              const std::string& scalar_t_name,
                                              int indent,
                                              std::ostream& out) {
      generate_indent(indent, out);
      generate_bare_type(fun.return_type_, scalar_t_name, out);
      out << EOL;
    }


  }
}
#endif
