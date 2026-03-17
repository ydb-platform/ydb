#ifndef STAN_OPTIMIZATION_LBFGS_UPDATE_HPP
#define STAN_OPTIMIZATION_LBFGS_UPDATE_HPP

#include <Eigen/Dense>
#include <boost/tuple/tuple.hpp>
#include <boost/circular_buffer.hpp>
#include <vector>

namespace stan {
  namespace optimization {
    /**
     * Implement a limited memory version of the BFGS update.  This
     * class maintains a circular buffer of inverse Hessian updates
     * which can be applied to compute the search direction.
     **/
    template<typename Scalar = double,
             int DimAtCompile = Eigen::Dynamic>
    class LBFGSUpdate {
    public:
      typedef Eigen::Matrix<Scalar, DimAtCompile, 1> VectorT;
      typedef Eigen::Matrix<Scalar, DimAtCompile, DimAtCompile> HessianT;
      // NOLINTNEXTLINE(build/include_what_you_use)
      typedef boost::tuple<Scalar, VectorT, VectorT> UpdateT;

      explicit LBFGSUpdate(size_t L = 5) : _buf(L) {}

      /**
       * Set the number of inverse Hessian updates to keep.
       *
       * @param L New size of buffer.
       **/
      void set_history_size(size_t L) {
        _buf.rset_capacity(L);
      }

      /**
       * Add a new set of update vectors to the history.
       *
       * @param yk Difference between the current and previous gradient vector.
       * @param sk Difference between the current and previous state vector.
       * @param reset Whether to reset the approximation, forgetting about
       * previous values.
       * @return In the case of a reset, returns the optimal scaling of the
       * initial Hessian
       * approximation which is useful for predicting step-sizes.
       **/
      inline Scalar update(const VectorT &yk, const VectorT &sk,
                           bool reset = false) {
        Scalar skyk = yk.dot(sk);

        Scalar B0fact;
        if (reset) {
          B0fact = yk.squaredNorm()/skyk;
          _buf.clear();
        } else {
          B0fact = 1.0;
        }

        // New updates are pushed to the "back" of the circular buffer
        Scalar invskyk = 1.0/skyk;
        _gammak = skyk/yk.squaredNorm();
        _buf.push_back();
        _buf.back() = boost::tie(invskyk, yk, sk);

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
        std::vector<Scalar> alphas(_buf.size());
        typename
          boost::circular_buffer<UpdateT>::const_reverse_iterator buf_rit;
        typename boost::circular_buffer<UpdateT>::const_iterator buf_it;
        typename std::vector<Scalar>::const_iterator alpha_it;
        typename std::vector<Scalar>::reverse_iterator alpha_rit;

        pk.noalias() = -gk;
        for (buf_rit = _buf.rbegin(), alpha_rit = alphas.rbegin();
             buf_rit != _buf.rend();
             buf_rit++, alpha_rit++) {
          Scalar alpha;
          const Scalar &rhoi(boost::get<0>(*buf_rit));
          const VectorT &yi(boost::get<1>(*buf_rit));
          const VectorT &si(boost::get<2>(*buf_rit));

          alpha = rhoi * si.dot(pk);
          pk -= alpha * yi;
          *alpha_rit = alpha;
        }
        pk *= _gammak;
        for (buf_it = _buf.begin(), alpha_it = alphas.begin();
             buf_it != _buf.end();
             buf_it++, alpha_it++) {
          Scalar beta;
          const Scalar &rhoi(boost::get<0>(*buf_it));
          const VectorT &yi(boost::get<1>(*buf_it));
          const VectorT &si(boost::get<2>(*buf_it));

          beta = rhoi*yi.dot(pk);
          pk += (*alpha_it - beta)*si;
        }
      }

    protected:
      boost::circular_buffer<UpdateT> _buf;
      Scalar _gammak;
    };
  }
}

#endif
