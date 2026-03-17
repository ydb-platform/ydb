#ifndef STAN_LANG_AST_NODE_INTEGRATE_1D_DEF_HPP
#define STAN_LANG_AST_NODE_INTEGRATE_1D_DEF_HPP

#include <stan/lang/ast.hpp>
#include <string>

namespace stan {
namespace lang {

integrate_1d::integrate_1d(const std::string& function_name,
                           const expression& lb, const expression& ub,
                           const expression& theta, const expression& x_r,
                           const expression& x_i, const expression& rel_tol) :
    function_name_(function_name), lb_(lb), ub_(ub), theta_(theta), x_r_(x_r),
    x_i_(x_i), rel_tol_(rel_tol) { }

integrate_1d::integrate_1d() { }

}
}
#endif
