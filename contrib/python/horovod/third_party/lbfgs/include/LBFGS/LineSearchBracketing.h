// Copyright (C) 2016 Yixuan Qiu <yixuan.qiu@cos.name> & Dirk Toewe <DirkToewe@GoogleMail.com>
// Under MIT license

#ifndef LINE_SEARCH_BRACKETING_H
#define LINE_SEARCH_BRACKETING_H

#include <Eigen/Core>
#include <stdexcept>  // std::runtime_error
#include <math.h>

namespace LBFGSpp {


///
/// Line search algorithms for LBFGS. Mainly for internal use.
///
template <typename Scalar>
class LineSearchBracketing
{
private:
    typedef Eigen::Matrix<Scalar, Eigen::Dynamic, 1> Vector;

public:
    ///
    /// Line search by bracketing. Similar to the backtracking line search
    /// except that is actively maintains an upper and lower bound of the
    /// current search range.
    ///
    /// \param f      A function object such that `f(x, grad)` returns the
    ///               objective function value at `x`, and overwrites `grad` with
    ///               the gradient.
    /// \param fx     In: The objective function value at the current point.
    ///               Out: The function value at the new point.
    /// \param x      Out: The new point moved to.
    /// \param grad   In: The current gradient vector. Out: The gradient at the
    ///               new point.
    /// \param step   In: The initial step length. Out: The calculated step length.
    /// \param drt    The current moving direction.
    /// \param xp     The current point.
    /// \param param  Parameters for the LBFGS algorithm
    ///
    template <typename Foo>
    static void LineSearch(Foo& f, Scalar& fx, Vector& x, Vector& grad,
                           Scalar& step,
                           const Vector& drt, const Vector& xp,
                           const LBFGSParam<Scalar>& param)
    {
        // Check the value of step
        if(step <= Scalar(0))
            std::invalid_argument("'step' must be positive");

        // Save the function value at the current x
        const Scalar fx_init = fx;
        // Projection of gradient on the search direction
        const Scalar dg_init = grad.dot(drt);
        // Make sure d points to a descent direction
        if(dg_init > 0)
            std::logic_error("the moving direction increases the objective function value");

        const Scalar dg_test = param.ftol * dg_init;

        // Upper and lower end of the current line search range
        Scalar step_lo = 0,
               step_hi = std::numeric_limits<Scalar>::infinity();

        for( int iter = 0; iter < param.max_linesearch; iter++ )
        {
            // x_{k+1} = x_k + step * d_k
            x.noalias() = xp + step * drt;
            // Evaluate this candidate
            fx = f(x, grad);

            if(fx > fx_init + step * dg_test)
            {
                step_hi = step;
            } else {
                // Armijo condition is met
                if(param.linesearch == LBFGS_LINESEARCH_BACKTRACKING_ARMIJO)
                    break;

                const Scalar dg = grad.dot(drt);
                if(dg < param.wolfe * dg_init)
                {
                    step_lo = step;
                } else {
                    // Regular Wolfe condition is met
                    if(param.linesearch == LBFGS_LINESEARCH_BACKTRACKING_WOLFE)
                        break;

                    if(dg > -param.wolfe * dg_init)
                    {
                        step_hi = step;
                    } else {
                        // Strong Wolfe condition is met
                        break;
                    }
                }
            }

            assert( step_lo < step_hi );

            if(iter >= param.max_linesearch)
                throw std::runtime_error("the line search routine reached the maximum number of iterations");

            if(step < param.min_step)
                throw std::runtime_error("the line search step became smaller than the minimum value allowed");

            if(step > param.max_step)
                throw std::runtime_error("the line search step became larger than the maximum value allowed");

            // continue search in mid of current search range
            step = std::isinf(step_hi) ? 2*step : step_lo/2 + step_hi/2;
        }
    }
};


} // namespace LBFGSpp

#endif // LINE_SEARCH_BRACKETING_H

