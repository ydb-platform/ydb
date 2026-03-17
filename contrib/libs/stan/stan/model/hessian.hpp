#ifndef STAN_MODEL_HESSIAN_HPP
#define STAN_MODEL_HESSIAN_HPP

#include <stan/math/mix/mat.hpp>
#include <stan/model/model_functional.hpp>
#include <iostream>

namespace stan {
  namespace model {

    template <class M>
    void hessian(const M& model,
                 const Eigen::Matrix<double, Eigen::Dynamic, 1>& x,
                 double& f,
                 Eigen::Matrix<double, Eigen::Dynamic, 1>& grad_f,
                 Eigen::Matrix<double, Eigen::Dynamic, Eigen::Dynamic>& hess_f,
                 std::ostream* msgs = 0) {
      stan::math::hessian<model_functional<M> >(model_functional<M>(model,
                                                                    msgs),
                                                x, f, grad_f, hess_f);
    }

  }
}
#endif
