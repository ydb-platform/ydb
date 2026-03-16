#ifndef STAN_OPTIMIZATION_BFGS_UPDATE_HPP
#define STAN_OPTIMIZATION_BFGS_UPDATE_HPP

#include <Eigen/Dense>

namespace stan {
  namespace optimization {
    template<typename Scalar = double,
             int DimAtCompile = Eigen::Dynamic>
    class BFGSUpdate_HInv {
    public:
      typedef Eigen::Matrix<Scalar, DimAtCompile, 1> VectorT;
      typedef Eigen::Matrix<Scalar, DimAtCompile, DimAtCompile> HessianT;

      /**
       * Update the inverse Hessian approximation.
       *
       * @param yk Difference between the current and previous gradient vector.
       * @param sk Difference between the current and previous state vector.
       * @param reset Whether to reset the approximation, forgetting about
       * previous values.
       * @return In the case of a reset, returns the optimal scaling of the
       * initial Hessian approximation which is useful for predicting
       * step-sizes.
       **/
      inline Scalar update(const VectorT &yk, const VectorT &sk,
                           bool reset = false) {
        Scalar rhok, skyk, B0fact;
        HessianT Hupd;

        skyk = yk.dot(sk);
        rhok = 1.0/skyk;

        Hupd.noalias() = HessianT::Identity(yk.size(), yk.size())
                                        - rhok * sk * yk.transpose();
        if (reset) {
          B0fact = yk.squaredNorm()/skyk;
          _Hk.noalias() = ((1.0/B0fact)*Hupd)*Hupd.transpose();
        } else {
          B0fact = 1.0;
          _Hk = Hupd*_Hk*Hupd.transpose();
        }
        _Hk.noalias() += rhok*sk*sk.transpose();
        return B0fact;
      }

      /**
       * Compute the search direction based on the current (inverse) Hessian
       * approximation and given gradient.
       *
       * @param[out] pk The negative product of the inverse Hessian and gradient
       * direction gk.
       * @param[in] gk Gradient direction.
       **/
      inline void search_direction(VectorT &pk, const VectorT &gk) const {
        pk.noalias() = -(_Hk*gk);
      }

    private:
      HessianT _Hk;
    };
  }
}

#endif
