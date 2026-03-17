#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTION_ARGUMENTS_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTION_ARGUMENTS_HPP

#include <stan/lang/ast.hpp>
#include <stan/lang/generator/constants.hpp>
#include <stan/lang/generator/generate_arg_decl.hpp>
#include <boost/lexical_cast.hpp>
#include <cstddef>
#include <ostream>
#include <string>

namespace stan {
  namespace lang {

    /**
     * Generate the arguments for the specified function, with
     * precalculated flags for whether it is an RNG, uses the log
     * density accumulator or is a probability function, to the
     * specified stream.
     *
     * @param[in] fun function declaration
     * @param[in] is_rng true if function is an RNG
     * @param[in] is_lp true if function accesses log density
     * accumulator
     * @param[in] is_log true if function is log probability function
     * @param[in,out] o stream for generating
     * @param[in] double_only do not do any templating and make all arguments
     * based on the standard double type
     * @param[in] rng_type set a type of the rng argument in _rng functions
     * @param[in] parameter_defaults if true, default values for the standard
     * parameters (now only pstream__) will be generated
     */
    void generate_function_arguments(const function_decl_def& fun, bool is_rng,
                                     bool is_lp, bool is_log, std::ostream& o,
                                     bool double_only = false,
                                     std::string rng_type = "RNG",
                                     bool parameter_defaults = false) {
      o << "(";
      for (size_t i = 0; i < fun.arg_decls_.size(); ++i) {
        std::string template_type_i;
        if (double_only) {
          template_type_i = "double";
        } else {
          template_type_i = "T" + boost::lexical_cast<std::string>(i) + "__";
        }
        generate_arg_decl(true, true, fun.arg_decls_[i], template_type_i, o);
        if (i + 1 < fun.arg_decls_.size()) {
          o << "," << EOL << INDENT;
          for (size_t i = 0; i <= fun.name_.size(); ++i)
            o << " ";
        }
      }
      if ((is_rng || is_lp) && fun.arg_decls_.size() > 0)
        o << ", ";
      if (is_rng) {
        o << rng_type << "& base_rng__";
      } else if (is_lp) {
        if (double_only) {
          o << "double& lp__, stan::math::accumulator<double>& lp_accum__";
        } else {
          o << "T_lp__& lp__, T_lp_accum__& lp_accum__";
        }
      }
      if (is_rng || is_lp || fun.arg_decls_.size() > 0)
        o << ", ";

      o << "std::ostream* pstream__";
      if (parameter_defaults) {
        o << " = nullptr";
      }

      o << ")";
    }

  }
}
#endif
