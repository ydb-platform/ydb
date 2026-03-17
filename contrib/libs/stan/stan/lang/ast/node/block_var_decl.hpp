#ifndef STAN_LANG_AST_NODE_BLOCK_VAR_DECL_HPP
#define STAN_LANG_AST_NODE_BLOCK_VAR_DECL_HPP

#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/var_decl.hpp>
#include <stan/lang/ast/type/block_var_type.hpp>
#include <string>

namespace stan {
namespace lang {

/**
 * Structure to hold a block variable declaration.
 * Block variables have optional constraints and
 * specified sizes.
 */
struct block_var_decl : public var_decl {
  /**
   * The line in the source code where the declaration begins.
   */
  std::size_t begin_line_;

  /**
   * The line in the source code where the declaration ends.
   */
  std::size_t end_line_;

  /**
   * Type-specific bounds (constraints) and sizes.
   */
  block_var_type type_;

  /**
   * Construct a default variable declaration.
   */
  block_var_decl();

  /**
   * Construct a block variable declaration with the specified
   * name and type.
   *
   * @param name variable name
   * @param type variable type
   */
  block_var_decl(const std::string& name, const block_var_type& type);

  /**
   * Construct a block variable declaration with the specified
   * name, type, and definition.
   *
   * @param name variable name
   * @param type variable type
   * @param def definition
   */
  block_var_decl(const std::string& name, const block_var_type& type,
                 const expression& def);

  /**
   * Return the variable declaration's bare expr type.
   *
   * @return the bare expr type
   */
  bare_expr_type bare_type() const;

  /**
   * Return the variable declaration's definition.
   *
   * @return expression definition for this variable
   */
  expression def() const;

  /**
   * Return true if variable declaration contains a definition.
   *
   * @return bool indicating has or doesn't have definition
   */
  bool has_def() const;

  /**
   * Return the variable declaration's name.
   *
   * @return name of variable
   */
  std::string name() const;

  /**
   * Return the variable declaration's block_var_type
   * which contains constraints and size specifications.
   *
   * @return block_var_type
   */
  block_var_type type() const;
};

}  // namespace lang
}  // namespace stan
#endif
