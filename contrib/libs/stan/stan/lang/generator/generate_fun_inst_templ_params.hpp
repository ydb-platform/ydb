#ifndef \
STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATION_TEMPLATE_PARAMETERS_HPP
#define \
STAN_LANG_GENERATOR_GENERATE_FUNCTION_INSTANTIATION_TEMPLATE_PARAMETERS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <ostream>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * Generate the concrete template parameters for function instantiation.
     *
     * @param[in] fun function declaration
     * @param[in] is_rng true if function is a random number generator
     * @param[in] is_lp true if function accesses log density
     * accumulator
     * @param[in] is_log true if function is a probability function
     * @param[in] rng_class class of the RNG being used (required by xxx_rng 
     * functions)
     * @param[in] out stream for generating
     */
    void generate_function_instantiation_template_parameters(
                                              const function_decl_def& fun,
                                              bool is_rng, bool is_lp,
                                              bool is_log,
                                              const std::string& rng_class,
                                              std::ostream& out) {
      std::vector<std::string> type_params;
      type_params.reserve(fun.arg_decls_.size());

      if (is_log) {
        std::string propto_value = "false";
        type_params.push_back(propto_value);
      }

      for (size_t i = 0; i < fun.arg_decls_.size(); ++i) {
        // no template parameter for int.innermost_type()d args
        if (!fun.arg_decls_[i].bare_type().innermost_type().is_int_type()) {
          type_params.push_back("double");
        }
      }
      if (is_rng) {
        type_params.push_back(rng_class);
      } else if (is_lp) {
        type_params.push_back("double");
        // the trailing space after '>' is necessary to compile
        type_params.push_back("stan::math::accumulator<double> ");
      }

      if (!type_params.empty()) {
        out << "<";
        for (size_t param_i = 0; param_i < type_params.size(); ++param_i) {
          if (param_i > 0)
            out << ", ";
          out << type_params[param_i];
        }
        out << ">";
      }
    }

  }
}
#endif
