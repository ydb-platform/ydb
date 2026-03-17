#ifndef STAN_LANG_AST_ROW_VECTOR_LOCAL_TYPE_DEF_HPP
#define STAN_LANG_AST_ROW_VECTOR_LOCAL_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
row_vector_local_type::row_vector_local_type(const expression& N) : N_(N) {}

row_vector_local_type::row_vector_local_type()
  : row_vector_local_type(nil()) {}

expression row_vector_local_type::N() const { return N_; }
}  // namespace lang
}  // namespace stan
#endif
