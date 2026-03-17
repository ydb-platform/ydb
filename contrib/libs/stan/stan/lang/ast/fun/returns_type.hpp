#ifndef STAN_LANG_AST_FUN_RETURNS_TYPE_HPP
#define STAN_LANG_AST_FUN_RETURNS_TYPE_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/statement.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Return true if the specified statement is a return statement
     * returning an expression of the specified type, otherwise return
     * false and write an error message to the specified error stream.
     *
     * @param[in] return_type expected type of returned expression
     * @param[in] statement statement to test
     * @param[in, out] error_msgs stream to which error messages are
     * written 
     * @return true if the specified statement is a return statement
     * with a return expression of the specified type
     */
    bool returns_type(const bare_expr_type& return_type,
                      const statement& statement,
                      std::ostream& error_msgs);

  }
}
#endif
