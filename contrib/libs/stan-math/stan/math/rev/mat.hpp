#ifndef STAN_MATH_REV_MAT_HPP
#define STAN_MATH_REV_MAT_HPP

#include <stan/math/rev/core.hpp>
#include <stan/math/rev/scal/meta/is_var.hpp>
#include <stan/math/rev/scal/meta/partials_type.hpp>
#include <stan/math/rev/mat/meta/operands_and_partials.hpp>

#include <stan/math/rev/mat/fun/Eigen_NumTraits.hpp>

#include <stan/math/rev/mat/vectorize/apply_scalar_unary.hpp>
#include <stan/math/prim/mat.hpp>
#include <stan/math/rev/arr.hpp>

#include <stan/math/rev/mat/fun/cholesky_decompose.hpp>
#include <stan/math/rev/mat/fun/columns_dot_product.hpp>
#include <stan/math/rev/mat/fun/columns_dot_self.hpp>
#include <stan/math/rev/mat/fun/cov_exp_quad.hpp>
#include <stan/math/rev/mat/fun/crossprod.hpp>
#include <stan/math/rev/mat/fun/determinant.hpp>
#include <stan/math/rev/mat/fun/divide.hpp>
#include <stan/math/rev/mat/fun/dot_product.hpp>
#include <stan/math/rev/mat/fun/dot_self.hpp>
#include <stan/math/rev/mat/fun/gp_periodic_cov.hpp>
#include <stan/math/rev/mat/fun/grad.hpp>
#include <stan/math/rev/mat/fun/initialize_variable.hpp>
#include <stan/math/rev/mat/fun/LDLT_alloc.hpp>
#include <stan/math/rev/mat/fun/LDLT_factor.hpp>
#include <stan/math/rev/mat/fun/log_determinant.hpp>
#include <stan/math/rev/mat/fun/log_determinant_ldlt.hpp>
#include <stan/math/rev/mat/fun/log_determinant_spd.hpp>
#include <stan/math/rev/mat/fun/log_softmax.hpp>
#include <stan/math/rev/mat/fun/log_sum_exp.hpp>
#include <stan/math/rev/mat/fun/matrix_exp_multiply.hpp>
#include <stan/math/rev/mat/fun/mdivide_left.hpp>
#include <stan/math/rev/mat/fun/mdivide_left_ldlt.hpp>
#include <stan/math/rev/mat/fun/mdivide_left_spd.hpp>
#include <stan/math/rev/mat/fun/mdivide_left_tri.hpp>
#include <stan/math/rev/mat/fun/multiply.hpp>
#include <stan/math/rev/mat/fun/multiply_lower_tri_self_transpose.hpp>
#include <stan/math/rev/mat/fun/ordered_constrain.hpp>
#include <stan/math/rev/mat/fun/positive_ordered_constrain.hpp>
#include <stan/math/rev/mat/fun/quad_form.hpp>
#include <stan/math/rev/mat/fun/quad_form_sym.hpp>
#include <stan/math/rev/mat/fun/rows_dot_product.hpp>
#include <stan/math/rev/mat/fun/scale_matrix_exp_multiply.hpp>
#include <stan/math/rev/mat/fun/sd.hpp>
#include <stan/math/rev/mat/fun/simplex_constrain.hpp>
#include <stan/math/rev/mat/fun/softmax.hpp>
#include <stan/math/rev/mat/fun/squared_distance.hpp>
#include <stan/math/rev/mat/fun/stan_print.hpp>
#include <stan/math/rev/mat/fun/sum.hpp>
#include <stan/math/rev/mat/fun/tcrossprod.hpp>
#include <stan/math/rev/mat/fun/to_var.hpp>
#include <stan/math/rev/mat/fun/trace_gen_inv_quad_form_ldlt.hpp>
#include <stan/math/rev/mat/fun/trace_gen_quad_form.hpp>
#include <stan/math/rev/mat/fun/trace_inv_quad_form_ldlt.hpp>
#include <stan/math/rev/mat/fun/trace_quad_form.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/variance.hpp>

#include <stan/math/rev/mat/functor/adj_jac_apply.hpp>
#include <stan/math/rev/mat/functor/algebra_solver.hpp>
#include <stan/math/rev/mat/functor/gradient.hpp>
#include <stan/math/rev/mat/functor/jacobian.hpp>
#include <stan/math/rev/mat/functor/cvodes_utils.hpp>
#include <stan/math/rev/mat/functor/cvodes_ode_data.hpp>
#include <stan/math/rev/mat/functor/integrate_ode_adams.hpp>
#include <stan/math/rev/mat/functor/integrate_ode_bdf.hpp>
#include <stan/math/rev/mat/functor/integrate_dae.hpp>
#include <stan/math/rev/mat/functor/map_rect_reduce.hpp>

#endif
