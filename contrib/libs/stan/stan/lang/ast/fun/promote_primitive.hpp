#ifndef STAN_LANG_AST_FUN_PROMOTE_PRIMITIVE_HPP
#define STAN_LANG_AST_FUN_PROMOTE_PRIMITIVE_HPP

#include <ostream>

namespace stan {
  namespace lang {

    struct bare_expr_type;

    /**
     * Primitive types are scalar `int` or `double`.
     * Single primitive type promotes (sic) to itself,
     * i.e., `int` promotes to `int`, `double` promotes to `double`.
     * Cannont promote non-primitive types to `int` or `double` so
     * function returns `ill_formed_type` for all other types.
     *
     * @param et expression type
     * @return promoted expression type
     */
    bare_expr_type promote_primitive(const bare_expr_type& et);

    /**
     * Promote pair of primitive types to `double` type when appropriate.
     * Pair (`int`, `double`) or (`double`, `int`) promotes to `double`.
     * Pair (`int`, `int`) promotes (sic) to `int`, likewise,
     * pair (`double`, `double`) promotes (sic) to `double`.
     * All other possible argument pairs have at least one non-primitive type,
     * therefore function returns `ill_formed_type`.
     *
     * @param et1 first expression type
     * @param et2 second expression type
     * @return promoted expression type
     */
    bare_expr_type promote_primitive(const bare_expr_type& et1,
                                     const bare_expr_type& et2);
  }
}
#endif
