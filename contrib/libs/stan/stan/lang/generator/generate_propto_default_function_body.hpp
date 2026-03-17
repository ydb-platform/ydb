#ifndef STAN_LANG_GENERATOR_GENERATE_PROPTO_DEFAULT_FUNCTION_BODY_HPP
#define STAN_LANG_GENERATOR_GENERATE_PROPTO_DEFAULT_FUNCTION_BODY_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the body of the specified function with
     * <code>propto</code> set to false, writing to the specified
     * stream.
     *
     * @param[in] fun function declaration
     * @param[in,out] o stream for generating
     */
    void generate_propto_default_function_body(const function_decl_def& fun,
                                               std::ostream& o) {
      o << " {" << EOL;
      o << INDENT << "return ";
      o << fun.name_ << "<false>(";
      for (size_t i = 0; i < fun.arg_decls_.size(); ++i) {
        if (i > 0)
          o << ",";
        o << fun.arg_decls_[i].name();
      }
      if (fun.arg_decls_.size() > 0)
        o << ", ";
      o << "pstream__";
      o << ");" << EOL;
      o << "}" << EOL;
    }


  }
}
#endif
