#ifndef STAN_LANG_AST_NODE_PROGRAM_HPP
#define STAN_LANG_AST_NODE_PROGRAM_HPP

#include <stan/lang/ast/node/function_decl_def.hpp>
#include <stan/lang/ast/node/statement.hpp>
#include <stan/lang/ast/node/block_var_decl.hpp>
#include <utility>
#include <vector>


namespace stan {
  namespace lang {

    /**
     * AST node for a complete Stan program.
     */
    struct program {
      /**
       * Type of a sequence of function declarations.
       */
      typedef std::vector<function_decl_def> function_decls_t;

      /**
       * Type of a sequence of variable declarations.
       */
      typedef std::vector<block_var_decl> block_var_decls_t;

      /**
       * Type of a sequence of statements.
       */
      typedef std::vector<statement> statements_t;

      /**
       * Type of pair of variable declaration sequence and statement sequence.
       */
      typedef std::pair<block_var_decls_t, statements_t> var_decls_statements_t;

      /**
       * Construct an uninitialized program.
       */
      program();

      /**
       * Construct a program with the specified components.
       *
       * @param[in] functions functions block
       * @param[in] data data block
       * @param[in] transformed_data transformed data block
       * @param[in] parameters parameters block
       * @param[in] transformed_parameters transformed parameters block
       * @param[in] model model block
       * @param[in] generated_quantities generated quantities block
       */
      program(const function_decls_t& functions,
              const block_var_decls_t& data,
              const var_decls_statements_t& transformed_data,
              const block_var_decls_t& parameters,
              const var_decls_statements_t& transformed_parameters,
              const statement& model,
              const var_decls_statements_t& generated_quantities);

      /**
       * Functions block.
       */
      std::vector<function_decl_def> function_decl_defs_;

      /**
       * Data block.
       */
      std::vector<block_var_decl> data_decl_;

      /**
       * Transformed data block.
       */
      std::pair<std::vector<block_var_decl>, std::vector<statement> >
        derived_data_decl_;

      /**
       * Parameters block.
       */
      std::vector<block_var_decl> parameter_decl_;

      /**
       * Transformed parameters block.
       */
      std::pair<std::vector<block_var_decl>,
                std::vector<statement> > derived_decl_;

      /**
       * Model block.
       */
      statement statement_;

      /**
       * Generated quantities block.
       */
      std::pair<std::vector<block_var_decl>,
                std::vector<statement> > generated_decl_;
    };

  }
}
#endif
