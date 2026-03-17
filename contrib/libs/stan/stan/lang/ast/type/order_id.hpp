#ifndef STAN_LANG_AST_TYPE_ORDER_ID_HPP
#define STAN_LANG_AST_TYPE_ORDER_ID_HPP

#include <string>

namespace stan {
namespace lang {

/**
 * String used to identify and impose lexicographic ordering on
 * variable and expression types.
 */
struct order_id {
  /**
   * String constant for variable and expression type.
   */
  static const std::string ORDER_ID;
};

}  // namespace lang
}  // namespace stan
#endif
