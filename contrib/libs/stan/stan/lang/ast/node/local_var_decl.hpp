#ifndef STAN_LANG_AST_NODE_LOCAL_VAR_DECL_HPP
#define STAN_LANG_AST_NODE_LOCAL_VAR_DECL_HPP

#include <stan/lang/ast/type/local_var_type.hpp>
#include <stan/lang/ast/node/expression.hpp>
#include <stan/lang/ast/node/var_decl.hpp>
#include <string>

namespace stan {
namespace lang {

/**
 * Structure to hold a local variable declaration.
 * Local variables have specified sizes.
 */
struct local_var_decl : public var_decl {
  /**
   * The line in the source code where the declaration begins.
   */
  std::size_t begin_line_;

  /**
   * The line in the source code where the declaration ends.
   */
  std::size_t end_line_;

  /**
   * Type-specific sizes.
   */
  local_var_type type_;

  /**
   * Construct a default variable declaration.
   */
  local_var_decl();

  /**
   * Construct a local variable declaration with the specified
   * name and type.
   *
   * @param name variable name
   * @param type variable type
   */
  local_var_decl(const std::string& name, const local_var_type& type);

  /**
   * Construct a local variable declaration with the specified
   * name, type, and definition.
   *
   * @param name variable name
   * @param type variable type
   * @param def definition
   */
  local_var_decl(const std::string& name, const local_var_type& type,
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
   * Return the variable declaration's local_var_type
   * which contains size specifications.
   *
   * @return local_var_type
   */
  local_var_type type() const;
};

}  // namespace lang
}  // namespace stan
#endif
