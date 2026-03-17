#ifndef STAN_MODEL_GRAD_TR_MAT_TIMES_HESSIAN_HPP
#define STAN_MODEL_GRAD_TR_MAT_TIMES_HESSIAN_HPP

#include <stan/model/model_functional.hpp>
#include <stan/math/mix/mat.hpp>
#include <ostream>

namespace stan {
  namespace model {

    template <class M>
    void grad_tr_mat_times_hessian(const M& model,
            const Eigen::Matrix <double, Eigen::Dynamic, 1>& x,
            const Eigen::Matrix <double, Eigen::Dynamic, Eigen::Dynamic>& X,
            Eigen::Matrix<double, Eigen::Dynamic, 1>&
            grad_tr_X_hess_f,
            std::ostream* msgs = 0) {
      stan::math::grad_tr_mat_times_hessian(model_functional<M>(model, msgs),
                                            x, X, grad_tr_X_hess_f);
    }

  }
}
#endif
