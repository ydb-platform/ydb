#ifndef STAN_LANG_AST_FUN_BARE_TYPE_TOTAL_DIMS_VIS_DEF_HPP
#define STAN_LANG_AST_FUN_BARE_TYPE_TOTAL_DIMS_VIS_DEF_HPP

#include <stan/lang/ast.hpp>

namespace stan {
namespace lang {
bare_type_total_dims_vis::bare_type_total_dims_vis() {}

int bare_type_total_dims_vis::operator()(const bare_array_type& x) const {
  return x.dims() + x.contains().num_dims();
}

int bare_type_total_dims_vis::operator()(const double_type& x) const {
  return 0;
}

int bare_type_total_dims_vis::operator()(const ill_formed_type& x) const {
  return 0;
}

int bare_type_total_dims_vis::operator()(const int_type& x) const {
  return 0;
}

int bare_type_total_dims_vis::operator()(const matrix_type& x) const {
  return 2;
}

int bare_type_total_dims_vis::operator()(const row_vector_type& x) const {
  return 1;
}

int bare_type_total_dims_vis::operator()(const vector_type& x) const {
  return 1;
}

int bare_type_total_dims_vis::operator()(const void_type& x) const {
  return 0;
}
}  // namespace lang
}  // namespace stan
#endif
