#ifndef STAN_LANG_GENERATOR_GENERATE_FUNCTOR_ARGUMENTS_HPP
#define STAN_LANG_GENERATOR_GENERATE_FUNCTOR_ARGUMENTS_HPP

#include <stan/lang/ast.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Generate the arguments for the functor for the specified
     * function declaration, with flags indicating whether it is a
     * random number generator, accesses the log density accumulator,
     * or is a probability function, writing to the specified stream.
     *
     * @param[in] fun function declaration
     * @param[in] is_rng true if function is a random number generator
     * @param[in] is_lp true if function acceses log density
     * accumulator
     * @param[in] is_log true if function is log probability function
     * @param[in,out] o stream for generating
     */
    void generate_functor_arguments(const function_decl_def& fun,  bool is_rng,
                                    bool is_lp, bool is_log, std::ostream& o) {
      o << "(";
      for (size_t i = 0; i < fun.arg_decls_.size(); ++i) {
        if (i > 0)
          o << ", ";
        o << fun.arg_decls_[i].name();
      }
      if ((is_rng || is_lp) && fun.arg_decls_.size() > 0)
        o << ", ";
      if (is_rng)
        o << "base_rng__";
      else if (is_lp)
        o << "lp__, lp_accum__";
      if (is_rng || is_lp || fun.arg_decls_.size() > 0)
        o << ", ";
      o << "pstream__";
      o << ")";
    }

  }
}
#endif
