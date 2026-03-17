#ifndef STAN_LANG_AST_NODE_RANGE_HPP
#define STAN_LANG_AST_NODE_RANGE_HPP

#include <stan/lang/ast/node/expression.hpp>

namespace stan {
  namespace lang {

    /**
     * AST structure for a range object with a low and high value.
     */
    struct range {
      /**
       * Lower bound of range with <code>nil</code> value if only
       * upper bound.
       */
      expression low_;

      /**
       * Upper bound of range with <code>nil</code> value if only
       * upper bound.
       */
      expression high_;

      /**
       * Construct a default range object.
       */
      range();

      /**
       * Construct a range object with the specified bounds.
       *
       * @param low lower bound
       * @param high upper bound
       */
      range(const expression& low, const expression& high);

      /**
       * Return true if the lower bound is non-nil.
       *
       * @return true if there is a lower bound
       */
      bool has_low() const;

      /**
       * Return true if the upper bound is non-nil.
       *
       * @return true if there is an upper bound
       */
      bool has_high() const;
    };

  }
}
#endif
