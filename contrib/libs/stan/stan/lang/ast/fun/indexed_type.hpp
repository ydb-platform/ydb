#ifndef STAN_LANG_AST_FUN_INDEXED_TYPE_HPP
#define STAN_LANG_AST_FUN_INDEXED_TYPE_HPP

#include <vector>

namespace stan {
namespace lang {

struct bare_expr_type;
struct expression;
struct idx;

/**
 * Return the type of the expression indexed by the generalized
 * index sequence.  Return a type with base type
 * <code>ill_formed_type</code> if there are too many indexes.
 *
 * @param[in] e Expression being indexed.
 * @param[in] idxs Index sequence.
 * @return Type of expression applied to indexes.
 */
bare_expr_type indexed_type(const expression& e, const std::vector<idx>& idxs);

}  // namespace lang
}  // namespace stan
#endif
