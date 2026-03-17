#ifndef STAN_OPTIMIZATION_BFGS_LINESEARCH_HPP
#define STAN_OPTIMIZATION_BFGS_LINESEARCH_HPP

#include <boost/math/special_functions/fpclassify.hpp>
#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <string>
#include <limits>

namespace stan {
  namespace optimization {
    /**
     * Find the minima in an interval [loX, hiX] of a cubic function which
     * interpolates the points, function values and gradients provided.
     *
     * Implicitly, this function constructs an interpolating polynomial
     *     g(x) = a_3 x^3 + a_2 x^2 + a_1 x + a_0
     * such that g(0) = 0, g(x1) = f1, g'(0) = df0, g'(x1) = df1 where
     *     g'(x) = 3 a_3 x^2 + 2 a_2 x + a_1
     * is the derivative of g(x).  It then computes the roots of g'(x) and
     * finds the minimal value of g(x) on the interval [loX,hiX] including
     * the end points.
     *
     * This function implements the full parameter version of CubicInterp().
     *
     * @param df0 First derivative value, f'(x0)
     * @param x1 Second point
     * @param f1 Second function value, f(x1)
     * @param df1 Second derivative value, f'(x1)
     * @param loX Lower bound on the interval of solutions
     * @param hiX Upper bound on the interval of solutions
     **/
    template<typename Scalar>
    Scalar CubicInterp(const Scalar &df0,
                       const Scalar &x1, const Scalar &f1, const Scalar &df1,
                       const Scalar &loX, const Scalar &hiX) {
      const Scalar c3((-12*f1 + 6*x1*(df0 + df1))/(x1*x1*x1));
      const Scalar c2(-(4*df0 + 2*df1)/x1 + 6*f1/(x1*x1));
      const Scalar &c1(df0);

      const Scalar t_s = std::sqrt(c2*c2 - 2.0*c1*c3);
      const Scalar s1 = - (c2 + t_s)/c3;
      const Scalar s2 = - (c2 - t_s)/c3;

      Scalar tmpF;
      Scalar minF, minX;

      // Check value at lower bound
      minF = loX*(loX*(loX*c3/3.0 + c2)/2.0 + c1);
      minX = loX;

      // Check value at upper bound
      tmpF = hiX*(hiX*(hiX*c3/3.0 + c2)/2.0 + c1);
      if (tmpF < minF) {
        minF = tmpF;
        minX = hiX;
      }

      // Check value of first root
      if (loX < s1 && s1 < hiX) {
        tmpF = s1*(s1*(s1*c3/3.0 + c2)/2.0 + c1);
        if (tmpF < minF) {
          minF = tmpF;
          minX = s1;
        }
      }

      // Check value of second root
      if (loX < s2 && s2 < hiX) {
        tmpF = s2*(s2*(s2*c3/3.0 + c2)/2.0 + c1);
        if (tmpF < minF) {
          minF = tmpF;
          minX = s2;
        }
      }

      return minX;
    }

    /**
     * Find the minima in an interval [loX, hiX] of a cubic function which
     * interpolates the points, function values and gradients provided.
     *
     * Implicitly, this function constructs an interpolating polynomial
     *     g(x) = a_3 x^3 + a_2 x^2 + a_1 x + a_0
     * such that g(x0) = f0, g(x1) = f1, g'(x0) = df0, g'(x1) = df1 where
     *     g'(x) = 3 a_3 x^2 + 2 a_2 x + a_1
     * is the derivative of g(x).  It then computes the roots of g'(x) and
     * finds the minimal value of g(x) on the interval [loX,hiX] including
     * the end points.
     *
     * @param x0 First point
     * @param f0 First function value, f(x0)
     * @param df0 First derivative value, f'(x0)
     * @param x1 Second point
     * @param f1 Second function value, f(x1)
     * @param df1 Second derivative value, f'(x1)
     * @param loX Lower bound on the interval of solutions
     * @param hiX Upper bound on the interval of solutions
     **/
    template<typename Scalar>
    Scalar CubicInterp(const Scalar &x0, const Scalar &f0, const Scalar &df0,
                       const Scalar &x1, const Scalar &f1, const Scalar &df1,
                       const Scalar &loX, const Scalar &hiX) {
      return x0 + CubicInterp(df0, x1-x0, f1-f0, df1, loX-x0, hiX-x0);
    }

    /**
     * An internal utility function for implementing WolfeLineSearch()
     **/
    template<typename FunctorType, typename Scalar, typename XType>
    int WolfLSZoom(Scalar &alpha, XType &newX, Scalar &newF, XType &newDF,
                   FunctorType &func,
                   const XType &x, const Scalar &f, const Scalar &dfp,
                   const Scalar &c1dfp, const Scalar &c2dfp, const XType &p,
                   Scalar alo, Scalar aloF, Scalar aloDFp,
                   Scalar ahi, Scalar ahiF, Scalar ahiDFp,
                   const Scalar &min_range) {
      Scalar d1, d2, newDFp;
      int itNum(0);

      while (1) {
        itNum++;

        if (std::fabs(alo-ahi) < min_range)
          return 1;

        if (itNum%5 == 0) {
          alpha = 0.5*(alo+ahi);
        } else {
          // Perform cubic interpolation to determine next point to try
          d1 = aloDFp + ahiDFp - 3*(aloF-ahiF)/(alo-ahi);
          d2 = std::sqrt(d1*d1 - aloDFp*ahiDFp);
          if (ahi < alo)
            d2 = -d2;
          alpha = ahi
            - (ahi - alo) * (ahiDFp + d2 - d1) / (ahiDFp - aloDFp + 2*d2);
          if (!boost::math::isfinite(alpha) ||
              alpha < std::min(alo, ahi) + 0.01 * std::fabs(alo - ahi) ||
              alpha > std::max(alo, ahi) - 0.01 * std::fabs(alo - ahi))
            alpha = 0.5 * (alo + ahi);
        }

        newX = x + alpha * p;
        while (func(newX, newF, newDF)) {
          alpha = 0.5 * (alpha + std::min(alo, ahi));
          if (std::fabs(std::min(alo, ahi) - alpha) < min_range)
            return 1;
          newX = x + alpha * p;
        }
        newDFp = newDF.dot(p);
        if (newF > (f + alpha * c1dfp) || newF >= aloF) {
          ahi = alpha;
          ahiF = newF;
          ahiDFp = newDFp;
        } else {
          if (std::fabs(newDFp) <= -c2dfp)
            break;
          if (newDFp*(ahi-alo) >= 0) {
            ahi = alo;
            ahiF = aloF;
            ahiDFp = aloDFp;
          }
          alo = alpha;
          aloF = newF;
          aloDFp = newDFp;
        }
      }
      return 0;
    }

    /**
     * Perform a line search which finds an approximate solution to:
     * \f[
     *       \min_\alpha f(x_0 + \alpha p)
     * \f]
     * satisfying the strong Wolfe conditions:
     *  1) \f$ f(x_0 + \alpha p) \leq f(x_0) + c_1 \alpha p^T g(x_0) \f$
     *  2) \f$ \vert p^T g(x_0 + \alpha p) \vert \leq c_2 \vert p^T g(x_0) \vert \f$
     * where \f$g(x) = \frac{\partial f}{\partial x}\f$ is the gradient of f(x).
     *
     * @tparam FunctorType A type which supports being called as
     *        ret = func(x,f,g)
     * where x is the input point, f and g are the function value and
     * gradient at x and ret is non-zero if function evaluation fails.
     *
     * @param func Function which is being minimized.
     *
     * @param alpha First value of \f$ \alpha \f$ to try.  Upon return this
     * contains the final value of the \f$ \alpha \f$.
     *
     * @param x1 Final point, equal to \f$ x_0 + \alpha p \f$.
     *
     * @param f1 Final point function value, equal to \f$ f(x_0 + \alpha p) \f$.
     *
     * @param gradx1 Final point gradient, equal to \f$ g(x_0 + \alpha p) \f$.
     *
     * @param p Search direction.  It is assumed to be a descent direction such
     * that \f$ p^T g(x_0) < 0 \f$.
     *
     * @param x0 Value of starting point, \f$ x_0 \f$.
     *
     * @param f0 Value of function at starting point, \f$ f(x_0) \f$.
     *
     * @param gradx0 Value of function gradient at starting point,
     *    \f$ g(x_0) \f$.
     *
     * @param c1 Parameter of the Wolfe conditions. \f$ 0 < c_1 < c_2 < 1 \f$
     * Typically c1 = 1e-4.
     *
     * @param c2 Parameter of the Wolfe conditions. \f$ 0 < c_1 < c_2 < 1 \f$
     * Typically c2 = 0.9.
     *
     * @param minAlpha Smallest allowable step-size.
     *
     * @param maxLSIts Maximum number line search iterations.
     *
     * @param maxLSRestarts Maximum number of times line search will
     * restart with \f$ f() \f$ failing.
     *
     * @return Returns zero on success, non-zero otherwise.
     **/
    template<typename FunctorType, typename Scalar, typename XType>
    int WolfeLineSearch(FunctorType &func,
                        Scalar &alpha,
                        XType &x1, Scalar &f1, XType &gradx1,
                        const XType &p,
                        const XType &x0, const Scalar &f0, const XType &gradx0,
                        const Scalar &c1, const Scalar &c2,
                        const Scalar &minAlpha, const Scalar &maxLSIts,
                        const Scalar &maxLSRestarts) {
      const Scalar dfp(gradx0.dot(p));
      const Scalar c1dfp(c1*dfp);
      const Scalar c2dfp(c2*dfp);

      Scalar alpha0(minAlpha);
      Scalar alpha1(alpha);

      Scalar prevF(f0);
      XType prevDF(gradx0);
      Scalar prevDFp(dfp);
      Scalar newDFp;

      int retCode = 0, nits = 0, lsRestarts = 0, ret;

      while (1) {
        if (nits >= maxLSIts) {
          retCode = 1;
          break;
        }

        x1.noalias() = x0 + alpha1 * p;
        ret = func(x1, f1, gradx1);
        if (ret != 0) {
          if (lsRestarts >= maxLSRestarts) {
            retCode = 1;
            break;
          }

          alpha1 = 0.5 * (alpha0 + alpha1);
          lsRestarts++;
          continue;
        }
        lsRestarts = 0;

        newDFp = gradx1.dot(p);
        if ((f1 > f0 + alpha * c1dfp) || (f1 >= prevF && nits > 0)) {
          retCode = WolfLSZoom(alpha, x1, f1, gradx1,
                               func,
                               x0, f0, dfp,
                               c1dfp, c2dfp, p,
                               alpha0, prevF, prevDFp,
                               alpha1, f1, newDFp,
                               1e-16);
          break;
        }
        if (std::fabs(newDFp) <= -c2dfp) {
          alpha = alpha1;
          break;
        }
        if (newDFp >= 0) {
          retCode = WolfLSZoom(alpha, x1, f1, gradx1,
                               func,
                               x0, f0, dfp,
                               c1dfp, c2dfp, p,
                               alpha1, f1, newDFp,
                               alpha0, prevF, prevDFp,
                               1e-16);
          break;
        }

        alpha0 = alpha1;
        prevF = f1;
        std::swap(prevDF, gradx1);
        prevDFp = newDFp;

        alpha1 *= 10.0;

        nits++;
      }
      return retCode;
    }
  }
}

#endif
