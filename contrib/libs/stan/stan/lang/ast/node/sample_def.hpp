#ifndef STAN_LANG_AST_NODE_SAMPLE_DEF_HPP
#define STAN_LANG_AST_NODE_SAMPLE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
  namespace lang {


    sample::sample() : is_discrete_(false) { }

    sample::sample(expression& e, distribution& dist)
      : expr_(e), dist_(dist), is_discrete_(false) { }

    bool sample::is_ill_formed() const {
        return expr_.bare_type().is_ill_formed_type()
          || (truncation_.has_low()
              && expr_.bare_type() != truncation_.low_.bare_type())
          || (truncation_.has_high()
               && expr_.bare_type()
                  != truncation_.high_.bare_type());
    }

    bool sample::is_discrete() const {
      return is_discrete_;
    }

  }
}
#endif
