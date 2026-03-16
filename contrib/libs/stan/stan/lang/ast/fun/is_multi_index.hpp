#ifndef STAN_LANG_AST_FUN_IS_MULTI_INDEX_HPP
#define STAN_LANG_AST_FUN_IS_MULTI_INDEX_HPP

#include <stan/lang/ast/node/idx.hpp>

namespace stan {
  namespace lang {

    /**
     * Return true if the specified index potentially takes more than
     * one value.
     *
     * @param idx index
     * @return true if index is not a unary index
     */
    bool is_multi_index(const idx& idx);

  }
}
#endif
