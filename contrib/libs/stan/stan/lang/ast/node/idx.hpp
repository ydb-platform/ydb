#ifndef STAN_LANG_AST_NODE_IDX_HPP
#define STAN_LANG_AST_NODE_IDX_HPP

#include <stan/lang/ast/node/lb_idx.hpp>
#include <stan/lang/ast/node/lub_idx.hpp>
#include <stan/lang/ast/node/multi_idx.hpp>
#include <stan/lang/ast/node/omni_idx.hpp>
#include <stan/lang/ast/node/ub_idx.hpp>
#include <stan/lang/ast/node/uni_idx.hpp>
#include <boost/variant/recursive_variant.hpp>
#include <string>

namespace stan {
  namespace lang {

    /**
     * AST variant structure for indexes, holding any of a unary,
     * multi, omni, lower-bound, upper-bound, or lower- and upper-bound
     * index. 
     */
    struct idx {
      /**
       * Variant type for the six index types.
       */
      typedef boost::variant<boost::recursive_wrapper<uni_idx>,
                             boost::recursive_wrapper<multi_idx>,
                             boost::recursive_wrapper<omni_idx>,
                             boost::recursive_wrapper<lb_idx>,
                             boost::recursive_wrapper<ub_idx>,
                             boost::recursive_wrapper<lub_idx> >
      idx_t;

      /**
       * Construct a default index.
       */
      idx();

      /**
       * Construct a unary index with specified index.
       *
       *@param i index
       */
      idx(const uni_idx& i);  // NOLINT(runtime/explicit)

      /**
       * Construct a multiple index with specified index.
       *
       *@param i index
       */
      idx(const multi_idx& i);  // NOLINT(runtime/explicit)

      /**
       * Construct a universal index with specified index.
       *
       *@param i index
       */
      idx(const omni_idx& i);  // NOLINT(runtime/explicit)

      /**
       * Construct a lower-bound index with specified index.
       *
       *@param i index
       */
      idx(const lb_idx& i);  // NOLINT(runtime/explicit)

      /**
       * Construct an upper-bound index with specified index.
       *
       *@param i index
       */
      idx(const ub_idx& i);  // NOLINT(runtime/explicit)

      /**
       * Construct a lower- and upper-bound index with specified index.
       *
       *@param i index
       */
      idx(const lub_idx& i);  // NOLINT(runtime/explicit)

      std::string to_string() const;

      /**
       * The index variant object.
       */
      idx_t idx_;
    };

  }
}
#endif

