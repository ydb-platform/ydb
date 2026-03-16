#ifndef STAN_LANG_AST_FUN_WRITE_BLOCK_VAR_TYPE_HPP
#define STAN_LANG_AST_FUN_WRITE_BLOCK_VAR_TYPE_HPP

#include <stan/lang/ast/type/block_var_type.hpp>
#include <ostream>

namespace stan {
  namespace lang {

    /**
     * Write a user-readable version of the specified variable type
     * to the specified output stream.
     *
     * @param o output stream
     * @param type variable type
     */
    std::ostream& write_block_var_type(std::ostream& o, block_var_type type);
  }
}
#endif
