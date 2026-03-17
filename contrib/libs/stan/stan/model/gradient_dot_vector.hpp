#ifndef STAN_MODEL_GRADIENT_DOT_VECTOR_HPP
#define STAN_MODEL_GRADIENT_DOT_VECTOR_HPP

#include <stan/math/mix/mat.hpp>
#include <stan/model/model_functional.hpp>
#include <iostream>

namespace stan {
  namespace model {

    template <class M>
    void gradient_dot_vector(const M& model,
                             const Eigen::Matrix<double, Eigen::Dynamic, 1>& x,
                             const Eigen::Matrix<double, Eigen::Dynamic, 1>& v,
                             double& f,
                             double& grad_f_dot_v,
                             std::ostream* msgs = 0) {
      stan::math::gradient_dot_vector(model_functional<M>(model, msgs),
                                      x, v, f, grad_f_dot_v);
    }


  }
}
#endif
