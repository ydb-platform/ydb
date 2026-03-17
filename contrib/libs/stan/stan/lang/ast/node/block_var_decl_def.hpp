#ifndef STAN_LANG_AST_NODE_BLOCK_VAR_DECL_DEF_HPP
#define STAN_LANG_AST_NODE_BLOCK_VAR_DECL_DEF_HPP

#include <stan/lang/ast/node/expression_def.hpp>
#include <stan/lang/ast/node/var_decl_def.hpp>
#include <string>

namespace stan {
namespace lang {

block_var_decl::block_var_decl() : var_decl("", ill_formed_type(), nil()),
                                   type_(ill_formed_type()) { }

block_var_decl::block_var_decl(const std::string& name,
                               const block_var_type& type)
  : var_decl(name, type.bare_type(), nil()), type_(type) { }

block_var_decl::block_var_decl(const std::string& name,
                               const block_var_type& type,
                               const expression& def)
  : var_decl(name, type.bare_type(), def), type_(type) { }

bare_expr_type block_var_decl::bare_type() const { return type_.bare_type(); }

expression block_var_decl::def() const { return def_; }

bool block_var_decl::has_def() const { return !is_nil(def_); }

std::string block_var_decl::name() const { return name_; }

block_var_type block_var_decl::type() const { return type_; }

}  // namespace lang
}  // namespace stan
#endif
