#ifndef STAN_LANG_AST_NODE_VAR_DECL_DEF_HPP
#define STAN_LANG_AST_NODE_VAR_DECL_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
  namespace lang {

    var_decl::var_decl()
      : name_(""), bare_type_(ill_formed_type()), def_(nil()) {  }

    var_decl::var_decl(const std::string& name)
      : name_(name), bare_type_(ill_formed_type()), def_(nil()) {  }

    var_decl::var_decl(const std::string& name,
                       const bare_expr_type& type)
      : name_(name), bare_type_(type), def_(nil()) {  }

    var_decl::var_decl(const std::string& name,
                       const bare_expr_type& type,
                       const expression& def)
      : name_(name), bare_type_(type), def_(def) {  }

    bare_expr_type var_decl::bare_type() const {
      return bare_type_;
    }

    expression var_decl::def() const {
      return def_;
    }

    std::string var_decl::name() const {
      return name_;
    }
  }
}

#endif
