#ifndef STAN_LANG_AST_FUN_IS_NIL_HPP
#define STAN_LANG_AST_FUN_IS_NIL_HPP

namespace stan {
  namespace lang {

    struct expression;

    /**
     * Return true if the specified expression is nil.
     *
     * @param e expression to test
     * @return true if expression is nil
     */
    bool is_nil(const expression& e);

  }
}
#endif
