#ifndef STAN_LANG_AST_FUN_INDEXED_TYPE_DEF_HPP
#define STAN_LANG_AST_FUN_INDEXED_TYPE_DEF_HPP

#include <stan/lang/ast.hpp>
#include <vector>

#include <iostream>

namespace stan {
namespace lang {

/*
 *  indexed_type
 *
 *  check each member of vector idxs
 *  multi-idx doesn't change indexed type
 *  uni-idx on array type reduces number of array dimensions
 *  uni-idx on vector/row vector reduces to double
 *  2 uni-idxs on matrix reduces to double
 *  1 uni-idx + 1 multi-idx on matrix reduces to
 *  vector or row_vector depending on position
 *
 */
bare_expr_type indexed_type(const expression& e, const std::vector<idx>& idxs) {
  // check idxs size, although parser should disallow this
  if (idxs.size() == 0)
    return e.bare_type();

  // cannot have more indexes than there are dimensions, even if they're multi
  int idx_sz = idxs.size();
  if (idx_sz > e.bare_type().num_dims())
    return ill_formed_type();

  // indexing starts with array dims 1 ... N
  int i = 0;
  int max_array_dims = e.bare_type().array_dims();
  int array_dims = max_array_dims;
  for (; i < max_array_dims && i < idx_sz; ++i) {
    if (!is_multi_index(idxs[i]))
      array_dims--;
  }
  if (i == idx_sz && array_dims == 0)
    return e.bare_type().innermost_type();
  if (i == idx_sz)
    return bare_array_type(e.bare_type().innermost_type(), array_dims);

  // index into vector/matrix
  size_t num_args = e.bare_type().num_dims() - e.bare_type().array_dims();
  std::vector<int> arg_slots(num_args, 0);
  for (size_t j = 0; j < arg_slots.size() && i < idx_sz; ++i, ++j) {
    if (!is_multi_index(idxs[i]))
      arg_slots[j] = 1;
  }
  // innermost type is vector/row_vector
  if (arg_slots.size() == 1 && arg_slots[0] == 1) {
    if (array_dims > 0)
      return bare_array_type(double_type(), array_dims);
    return double_type();
  }
  if (arg_slots.size() == 1) {
    if (array_dims > 0)
      return bare_array_type(e.bare_type().innermost_type(), array_dims);
    return e.bare_type().innermost_type();
  }
  // innermost type is matrix
  if (arg_slots[0] == 1 && arg_slots[1] == 1) {
    if (array_dims > 0)
      return bare_array_type(double_type(), array_dims);
    return double_type();
  }
  if (arg_slots[0] == 1) {
    if (array_dims > 0)
      return bare_array_type(row_vector_type(), array_dims);
    return row_vector_type();
  }
  if (arg_slots[1] == 1) {
    if (array_dims > 0)
      return bare_array_type(vector_type(), array_dims);
    return vector_type();
  }
  if (array_dims > 0)
    return bare_array_type(matrix_type(), array_dims);
  return matrix_type();
}

}  // namespace lang
}  // namespace stan
#endif
