#ifndef STAN_LANG_AST_NODE_LOCAL_VAR_DECL_DEF_HPP
#define STAN_LANG_AST_NODE_LOCAL_VAR_DECL_DEF_HPP

#include <stan/lang/ast/node/expression_def.hpp>
#include <stan/lang/ast/node/var_decl_def.hpp>
#include <string>

namespace stan {
namespace lang {

local_var_decl::local_var_decl()
    : var_decl("", ill_formed_type(), nil()), type_(ill_formed_type()) {}

local_var_decl::local_var_decl(const std::string& name,
                               const local_var_type& type)
  : var_decl(name, type.bare_type(), nil()), type_(type) {}

local_var_decl::local_var_decl(const std::string& name,
                               const local_var_type& type,
                               const expression& def)
    : var_decl(name, type.bare_type(), def), type_(type) {}

bare_expr_type local_var_decl::bare_type() const {
  return type_.bare_type();
}

expression local_var_decl::def() const { return def_; }

bool local_var_decl::has_def() const { return !is_nil(def_); }

std::string local_var_decl::name() const { return name_; }

local_var_type local_var_decl::type() const { return type_; }
}  // namespace lang
}  // namespace stan
#endif
