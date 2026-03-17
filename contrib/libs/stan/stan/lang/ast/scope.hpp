#ifndef STAN_LANG_AST_SCOPE_HPP
#define STAN_LANG_AST_SCOPE_HPP


#include <stan/lang/ast/origin_block.hpp>
#include <cstddef>

namespace stan {
  namespace lang {

    /**
     * Structure which tracks enclosing program block(s) encountered by parser.
     * Var_map records program block where variable declared.
     * Grammar rules check allowed constructs in (enclosing) block.
     */
    class scope {
      /**
       * Outermost enclosing program block.
       */
      origin_block program_block_;

      /**
       * True if in a nested (local) program block.
       */
      bool is_local_;

    public:
      /**
       * No arg constructor, defaults:
       * - \p program_block_ : model_name_origin
       * - \p is_local_ : \c false
       */
      scope();

      /**
       * Construct an origin for variable in a specified block.
       * Default \c is_local is \c false, i.e., not in a local block.
       *
       * @param program_block enclosing program block
       */
      scope(const origin_block& program_block);   // NOLINT(runtime/explicit)

      /**
       * Construct scope for a variable in specified outer program block,
       * specify whether or not variable is declared in a local block.
       *
       * @param program_block enclosing program block
       * @param is_local true if declared in a local block
       */
      scope(const origin_block& program_block,
                 const bool& is_local);

      /**
       * Return value for outermost enclosing program block.
       *
       * @return program_block enclosing program block
       */
      origin_block program_block() const;

      /**
       * Return true when declared in a nested (local) block,
       * enclosing block can be any \c origin_block value.
       *
       * @return true when scope is nested (local) block.
       */
      bool is_local() const;

      /**
       * Flags local scopes which permit parameter variables.
       * Allows local blocks in functions, transfromed parameter,
       * and model blocks; disallows local blocks in transformed data
       * and generated quantities program blocks.
       *
       * @return true for local parameter origin block types
       */
      bool local_allows_var() const;

      /**
       * Flags scopes where parameter variables are declared,
       * i.e., top-level of parameter or transformed parameter block.
       *
       * @return true for top-level parameter origin block types
       */
      bool par_or_tpar() const;

      /**
       * Return true when declared in transformed parameter block.
       *
       * @return true for transformed parameter origin block
       */
      bool tpar() const;

      /**
       * Return true when declared as function argument.
       *
       * @return true for function origin block types
       */
      bool fun() const;

      /**
       * Return true when declared as argument to non-void function.
       *
       * @return true for non void function origin block types
       */
      bool non_void_fun() const;

      /**
       * Return true when declared as argument to void function.
       *
       * @return true for void function origin block types
       */
      bool void_fun() const;

      /**
       * Return true when program block allows assignment to variables
       * i.e., not data or parameter block
       *
       * @return true when program block allows access to LP
       */
      bool allows_assignment() const;

      /**
       * Return true when program block allows access to LP function
       *
       * @return true when program block allows access to LP function
       */
      bool allows_lp_fun() const;

      /**
       * Return true when program block allows access to RNG
       * i.e., transformed data block or rng function
       *
       * @return true when program block allows access to RNG
       */
      bool allows_rng() const;

      /**
       * Return true when program block allows access to sampling statement
       *
       * @return true when program block allows access to sampling statement
       */
      bool allows_sampling() const;

      /**
       * Returns true for origin blocks where size-denoting expression
       * declarations are allowed.  Origin blocks not allowed:
       *  - parameters
       *  - transformed parameters
       *  - generated quantities
       *
       * @return true if origin block allows size-denoting variable declaration.
       */
      bool allows_size() const;
    };

  }
}
#endif
