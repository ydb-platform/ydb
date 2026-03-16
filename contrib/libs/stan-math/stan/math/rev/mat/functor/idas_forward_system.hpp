#ifndef STAN_MATH_REV_MAT_FUNCTOR_IDAS_FORWARD_SYSTEM_HPP
#define STAN_MATH_REV_MAT_FUNCTOR_IDAS_FORWARD_SYSTEM_HPP

#include <stan/math/prim/arr/fun/value_of.hpp>
#include <stan/math/prim/scal/err/check_greater.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/mat/functor/idas_system.hpp>
#include <stan/math/rev/mat/functor/jacobian.hpp>
#include <idas/idas.h>
#include <nvector/nvector_serial.h>
#include <ostream>
#include <vector>

namespace stan {
namespace math {

/**
 * IDAS DAE system with forward sensitivity calculation
 *
 * @tparam F type of functor for DAE residual.
 * @tparam Tyy type of initial unknown values.
 * @tparam Typ type of initial unknown's derivative values.
 * @tparam Tpar type of parameters.
 */
template <typename F, typename Tyy, typename Typ, typename Tpar>
class idas_forward_system : public idas_system<F, Tyy, Typ, Tpar> {
  N_Vector* nv_yys_;
  N_Vector* nv_yps_;

 public:
  /**
   * Construct IDAS DAE system from initial condition and parameters
   *
   * @param[in] f DAE residual functor
   * @param[in] eq_id array for DAE's variable ID, it is a
   * reference to a constant vector with 1 or 0 as member
   * entries. 1 for derivative variables, 0 for algebraic variables.
   * @param[in] yy0 initial condition
   * @param[in] yp0 initial condition for derivatives
   * @param[in] theta parameters of the base DAE
   * @param[in] x_r continuous data vector for the DAE
   * @param[in] x_i integer data vector for the DAE
   * @param[in] msgs stream to which messages are printed
   */
  idas_forward_system(const F& f, const std::vector<int>& eq_id,
                      const std::vector<Tyy>& yy0, const std::vector<Typ>& yp0,
                      const std::vector<Tpar>& theta,
                      const std::vector<double>& x_r,
                      const std::vector<int>& x_i, std::ostream* msgs)
      : idas_system<F, Tyy, Typ, Tpar>(f, eq_id, yy0, yp0, theta, x_r, x_i,
                                       msgs) {
    if (this->need_sens) {
      nv_yys_ = N_VCloneVectorArray(this->ns_, this->nv_yy_);
      nv_yps_ = N_VCloneVectorArray(this->ns_, this->nv_yp_);
      for (size_t is = 0; is < this->ns_; ++is) {
        N_VConst(RCONST(0.0), nv_yys_[is]);
        N_VConst(RCONST(0.0), nv_yps_[is]);
      }
    }
  }

  /**
   * destructor to deallocate IDAS solution memory and workspace.
   */
  ~idas_forward_system() {
    if (this->need_sens) {
      N_VDestroyVectorArray_Serial(this->nv_yys_, this->ns_);
      N_VDestroyVectorArray_Serial(this->nv_yps_, this->ns_);
    }
  }

  /**
   * return N_Vector pointer array of sensitivity
   */
  N_Vector* nv_yys() { return nv_yys_; }

  /**
   * return N_Vector pointer array of sensitivity time derivative
   */
  N_Vector* nv_yps() { return nv_yps_; }

  /**
   * convert to void pointer for IDAS callbacks
   */
  void* to_user_data() {  // prepare to inject DAE info
    return static_cast<void*>(this);
  }

  /**
   * return a lambda for sensitivity residual callback.
   */
  IDASensResFn sensitivity_residual() const {
    return [](int ns, double t, N_Vector yy, N_Vector yp, N_Vector res,
              N_Vector* yys, N_Vector* yps, N_Vector* ress, void* user_data,
              N_Vector temp1, N_Vector temp2, N_Vector temp3) {
      using Eigen::Matrix;
      using Eigen::MatrixXd;
      using Eigen::VectorXd;
      using Eigen::Dynamic;

      using DAE = idas_forward_system<F, Tyy, Typ, Tpar>;
      DAE* dae = static_cast<DAE*>(user_data);

      static const char* caller = "sensitivity_residual";
      check_greater(caller, "number of parameters", ns, 0);

      const size_t& N = dae->N_;
      const size_t& M = dae->M_;

      Eigen::Map<VectorXd> vec_yy(N_VGetArrayPointer(yy), N);
      Eigen::Map<VectorXd> vec_yp(N_VGetArrayPointer(yp), N);
      std::vector<double> vyy(vec_yy.data(), vec_yy.data() + N);
      std::vector<double> vyp(vec_yp.data(), vec_yp.data() + N);
      std::vector<double> vtheta = value_of(dae->theta());

      std::vector<double> vpar = value_of(dae->theta_);
      Eigen::Map<VectorXd> vec_par(vpar.data(), vpar.size());

      auto yys_mat = matrix_d_from_NVarray(yys, ns);
      auto yps_mat = matrix_d_from_NVarray(yps, ns);

      try {
        stan::math::start_nested();

        MatrixXd J, r;
        VectorXd f_val;

        auto fyy
            = [&t, &vyp, &vtheta, &N, &dae](const matrix_v& x) -> vector_v {
          std::vector<var> yy(x.data(), x.data() + N);
          auto eval
              = dae->f_(t, yy, vyp, vtheta, dae->x_r_, dae->x_i_, dae->msgs_);
          Eigen::Map<vector_v> res(eval.data(), N);
          return res;
        };
        stan::math::jacobian(fyy, vec_yy, f_val, J);
        r = J * yys_mat;

        auto fyp
            = [&t, &vyy, &vtheta, &N, &dae](const matrix_v& x) -> vector_v {
          std::vector<var> yp(x.data(), x.data() + N);
          auto eval
              = dae->f_(t, vyy, yp, vtheta, dae->x_r_, dae->x_i_, dae->msgs_);
          Eigen::Map<vector_v> res(eval.data(), N);
          return res;
        };
        stan::math::jacobian(fyp, vec_yp, f_val, J);
        r += J * yps_mat;

        if (dae->is_var_par) {
          auto fpar
              = [&t, &vyy, &vyp, &N, &M, &dae](const matrix_v& x) -> vector_v {
            std::vector<var> par(x.data(), x.data() + M);
            auto eval
                = dae->f_(t, vyy, vyp, par, dae->x_r_, dae->x_i_, dae->msgs_);
            Eigen::Map<vector_v> res(eval.data(), N);
            return res;
          };
          stan::math::jacobian(fpar, vec_par, f_val, J);
          r.block(0, (dae->is_var_yy0 ? N : 0) + (dae->is_var_yp0 ? N : 0), N,
                  M)
              += J;  // only for theta
        }

        matrix_d_to_NVarray(r, ress, ns);
      } catch (const std::exception& e) {
        stan::math::recover_memory_nested();
        throw;
      }

      stan::math::recover_memory_nested();

      return 0;
    };
  }
};

}  // namespace math
}  // namespace stan

#endif
