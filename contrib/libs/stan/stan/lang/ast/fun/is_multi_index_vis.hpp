#ifndef STAN_LANG_AST_FUN_IS_MULTI_INDEX_VIS_HPP
#define STAN_LANG_AST_FUN_IS_MULTI_INDEX_VIS_HPP

#include <stan/lang/ast/node/lb_idx.hpp>
#include <stan/lang/ast/node/lub_idx.hpp>
#include <stan/lang/ast/node/multi_idx.hpp>
#include <stan/lang/ast/node/omni_idx.hpp>
#include <stan/lang/ast/node/ub_idx.hpp>
#include <stan/lang/ast/node/uni_idx.hpp>
#include <boost/variant/static_visitor.hpp>

namespace stan {
  namespace lang {

    /**
     * Visitor for callback to determine if an index is a multiple
     * index or a single index.
     */
    struct is_multi_index_vis : public boost::static_visitor<bool> {
      /**
       * Construct a multi-index visitor.
       */
      is_multi_index_vis();

      /**
       * Return false.
       *
       * @param i index
       */
      bool operator()(const uni_idx& i) const;

      /**
       * Return true.
       *
       * @param i index
       */
      bool operator()(const multi_idx& i) const;

      /**
       * Return true.
       *
       * @param i index
       */
      bool operator()(const omni_idx& i) const;

      /**
       * Return true.
       *
       * @param i index
       */
      bool operator()(const lb_idx& i) const;

      /**
       * Return true.
       *
       * @param i index
       */
      bool operator()(const ub_idx& i) const;

      /**
       * Return true.
       *
       * @param i index
       */
      bool operator()(const lub_idx& i) const;
    };
  }
}
#endif
