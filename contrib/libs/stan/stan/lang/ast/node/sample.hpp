#ifndef STAN_LANG_AST_NODE_SAMPLE_HPP
#define STAN_LANG_AST_NODE_SAMPLE_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/distribution.hpp>
#include <stan/lang/ast/node/range.hpp>
#include <utility>
#include <vector>


namespace stan {
  namespace lang {

    /**
     * AST node for sampling statements.
     */
    struct sample {
      /**
       * Construct an uninitialized sampling statement.
       */
      sample();

      /**
       * Construct a sampling statement with the specified variate and
       * distribution.  
       *
       * @param e random variate
       * @param dist distribution for variate
       */
      sample(expression& e, distribution& dist);

      /**
       * Return true if the sampling statement is not well formed.
       *
       * @return true if sampling statement is not well formed
       */
      bool is_ill_formed() const;

      /**
       * Return true if the distribution is discrete.
       *
       * @return true if the distribution is discrete.
       */
      bool is_discrete() const;

      /**
       * The random variable.
       */
      expression expr_;

      /**
       * Distribution of the variable.
       */
      distribution dist_;

      /**
       * The truncation range for the distribution.
       */
      range truncation_;

      /**
       * Discreteness flag.
       */
      bool is_discrete_;
    };

  }
}
#endif
