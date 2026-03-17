#ifndef STAN_LANG_AST_MATRIX_LOCAL_TYPE_DEF_HPP
#define STAN_LANG_AST_MATRIX_LOCAL_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
matrix_local_type::matrix_local_type(const expression& M, const expression& N)
    : M_(M), N_(N) {}

matrix_local_type::matrix_local_type() : matrix_local_type(nil(), nil()) {}

expression matrix_local_type::M() const { return M_; }

expression matrix_local_type::N() const { return N_; }
}  // namespace lang
}  // namespace stan
#endif
