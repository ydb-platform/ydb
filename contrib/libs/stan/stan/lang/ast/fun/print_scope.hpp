#ifndef STAN_LANG_AST_FUN_PRINT_SCOPE_HPP
#define STAN_LANG_AST_FUN_PRINT_SCOPE_HPP

#include <stan/lang/ast/scope.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Write a user-readable version of the specified variable scope
     * to the specified output stream.
     *
     * @param o output stream
     * @param var_scope variable scope
     */
    void print_scope(std::ostream& o, const scope& var_scope);

  }
}
#endif
