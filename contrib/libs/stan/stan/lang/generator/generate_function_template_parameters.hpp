#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_TEMPLATE_PARAMETERS_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_TEMPLATE_PARAMETERS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the template declaration for functions.
     *
     * @param[in] fun function declaration
     * @param[in] is_rng true if function is a random number generator
     * @param[in] is_lp true if function accesses log density
     * accumulator
     * @param[in] is_log true if function is a probability function
     * @param[in] out stream for generating
     */
    void generate_function_template_parameters(const function_decl_def& fun,
                                               bool is_rng, bool is_lp,
                                               bool is_log, std::ostream& out) {
      if (!fun.has_only_int_args()) {  // other cases handled below
        out << "template <";
        bool continuing_tps = false;
        if (is_log) {
          out << "bool propto";
          continuing_tps = true;
        }
        for (size_t i = 0; i < fun.arg_decls_.size(); ++i) {
          // no template parameter for int based args
          if (!fun.arg_decls_[i].bare_type().innermost_type().is_int_type()) {
            if (continuing_tps)
              out << ", ";
            out << "typename T" << i << "__";
            continuing_tps = true;
          }
        }
        if (is_rng) {
          if (continuing_tps)
            out << ", ";
          out << "class RNG";
          continuing_tps = true;
        } else if (is_lp) {
          if (continuing_tps)
            out << ", ";
          out << "typename T_lp__, typename T_lp_accum__";
          continuing_tps = true;
        }
        out << ">" << EOL;
      } else {  // no-arg function
        if (is_rng) {
          // nullary RNG case
          out << "template <class RNG>" << EOL;
        } else if (is_lp) {
          out << "template <typename T_lp__, typename T_lp_accum__>"
              << EOL;
        } else if (is_log) {
          out << "template <bool propto>"
              << EOL;
        }
      }
    }

  }
}
#endif
