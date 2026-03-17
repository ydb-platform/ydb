#ifndef STAN_LANG_AST_NODE_FUNCTION_DECL_DEF_DEF_HPP
#define STAN_LANG_AST_NODE_FUNCTION_DECL_DEF_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>
#include <vector>

namespace stan {
  namespace lang {

    function_decl_def::function_decl_def() { }

    function_decl_def::function_decl_def(const bare_expr_type& return_type,
                                         const std::string& name,
                                         const std::vector<var_decl>& arg_decls,
                                         const statement& body)
      : return_type_(return_type), name_(name), arg_decls_(arg_decls),
        body_(body) {
    }

    bool function_decl_def::has_only_int_args() const {
      for (size_t i = 0; i < arg_decls_.size(); ++i)
        if (!arg_decls_[i].bare_type().innermost_type().is_int_type())
          return false;
      return true;
    }

  }
}
#endif
