#ifndef STAN_LANG_AST_NODE_FUNCTION_DECL_DEF_HPP
#define STAN_LANG_AST_NODE_FUNCTION_DECL_DEF_HPP

#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/var_decl.hpp>
#include <stan/lang/ast/node/statement.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    /**
     * AST node for a function declaration and definition including
     * return type name, arguments, and body.
     */
    struct function_decl_def {
      /**
       * Construct an uninitialized function declaration and
       * definition. 
       */
      function_decl_def();

      /**
       * Construct a function declaration and definition with the
       * specified return type, function name, argument declarations
       * and function body.
       *
       * @param[in] return_type type of return value of function
       * @param[in] name function name
       * @param[in] arg_decls sequence of argument declarations
       * @param[in] body function body
       * 
       */
      function_decl_def(const bare_expr_type& return_type,
                        const std::string& name,
                        const std::vector<var_decl>& arg_decls,
                        const statement& body);

      /**
       * Type of value returned by function.
       */
      bare_expr_type return_type_;

      /**
       * Name of the function.
       */
      std::string name_;

      /**
       * Sequence of argument declarations.
       */
      std::vector<var_decl> arg_decls_;

      /**
       * Body of the function.
       */
      statement body_;

      /**
       * Return true if this function has only integer arguments.
       */
      bool has_only_int_args() const;
    };

  }
}
#endif
