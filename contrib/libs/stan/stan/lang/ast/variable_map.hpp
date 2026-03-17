#ifndef STAN_LANG_AST_VARIABLE_MAP_HPP
#define STAN_LANG_AST_VARIABLE_MAP_HPP

#include <stan/lang/ast/scope.hpp>
#include <stan/lang/ast/type/bare_expr_type.hpp>
#include <stan/lang/ast/node/var_decl.hpp>
#include <cstddef>
#include <map>
#include <string>
#include <utility>

namespace stan {
  namespace lang {

    /**
     * A map from variable names to their declarations and their scope.
     */
    struct variable_map {
      /**
       * A variable type and the scope of its declaration.
       */
      typedef std::pair<var_decl, scope> range_t;

      /**
       * Return true if a variable has been declared with the
       * specified name. 
       *
       * @param name variable name
       * @return true if the variable has been declared
       */
      bool exists(const std::string& name) const;

      /**
       * Return the type for the variable with the specified name. 
       *
       * @param name variable name
       * @return base declaration for variable with the specified name 
       * @throw std::invalid_argument if the variable has not been
       * declared 
       */
      var_decl get(const std::string& name) const;

      /**
       * Return the type declared for the variable with the specified
       * name. 
       *
       * @param name variable name
       * @return bare var type
       * @throw std::invalid_argument if the variable has not been
       * declared 
       */
      bare_expr_type get_bare_type(const std::string& name) const;

      /**
       * Return the scope in which the variable is declared for the
       * variable with the specified name.
       *
       * @param name variable name
       * @return scope of the variable
       * @throw std::invalid_argument if the variable has not been
       * declared 
       */
      scope get_scope(const std::string& name) const;

      /**
       * Add the specified declaration for a variable with the
       * specified name in the specified scope.
       * Destructively overwrites the declaration of an existing
       * variable if called with a new declaration and scope. 
       *
       * @param name variable name
       * @param var_decl variable declaration
       * @param scope_decl declaration scope 
       */
      void add(const std::string& name,
               const var_decl& var_decl,
               const scope& scope_decl);

      /**
       * Remove the declaraiton for the variable with the specified
       * name.  If the variable had not already been declared, it
       * the function exits silently.
       *
       * @param name name of variable to remove
       */
      void remove(const std::string& name);

      /**
       * The stored map from function names to their declarations and
       * origins.
       */
      std::map<std::string, range_t> map_;

      size_t size() const;
    };
  }
}
#endif
