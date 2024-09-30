/*******************************************************************************
 * tlx/math/polynomial_regression.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2018 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_MATH_POLYNOMIAL_REGRESSION_HEADER
#define TLX_MATH_POLYNOMIAL_REGRESSION_HEADER

#include <cmath>
#include <cstdlib>
#include <vector>

namespace tlx {

//! \addtogroup tlx_math
//! \{

/*!
 * Calculate the regression polynomial \f$ a_0+a_1x^1+a_2x^2+\cdots+a_nx^n \f$
 * from a list of 2D points.
 * See also https://en.wikipedia.org/wiki/Polynomial_regression
 *
 * If WithStore is false, then the sums are aggregated directly such that the
 * class retains O(1) size independent of the number of points. if WithStore is
 * true then the points are stored in a vector and can be retrieved.
 */
template <typename Type = double, bool WithStore = false>
class PolynomialRegression
{
public:
    //! start new polynomial regression calculation
    PolynomialRegression(size_t order)
        : order_(order), size_(0), X_(2 * order + 1, 0), Y_(order + 1, 0)
    { }

    //! number of points
    size_t size() const {
        return size_;
    }

    //! 2D point
    struct Point {
        Type x, y;
    };

    //! add point. this invalidates cached coefficients until next evaluate()
    PolynomialRegression& add(const Type& x, const Type& y) {
        X_[0] += 1.0;
        if (1 < 2 * order_ + 1)
            X_[1] += x;
        if (2 < 2 * order_ + 1)
            X_[2] += x * x;
        for (size_t i = 3; i < 2 * order_ + 1; ++i)
            X_[i] += Type(std::pow(x, i));

        Y_[0] += y;
        if (1 < order_ + 1)
            Y_[1] += x * y;
        if (2 < order_ + 1)
            Y_[2] += x * x * y;
        for (size_t i = 3; i < order_ + 1; ++i)
            Y_[i] += Type(std::pow(x, i)) * y;

        if (WithStore)
            points_.emplace_back(Point { x, y });

        ++size_;
        coefficients_.clear();
        return *this;
    }

    //! return a point. Only available if WithStore is true.
    const Point& point(size_t i) {
        return points_[i];
    }

    //! polynomial stored as the coefficients of
    //! \f$ a_0+a_1 x^1+a_2 x^2+\cdots+a_n x^n \f$
    struct Coefficients : public std::vector<Type> {
        //! evaluate polynomial at x using Horner schema
        Type evaluate(const Type& x) const {
            Type result = 0.0;
            for (size_t i = this->size(); i != 0; ) {
                result = result * x + this->operator [] (--i);
            }
            return result;
        }
    };

    //! get r^2. Only available if WithStore is true.
    Type r_square() {
        if (size_ == 0 || !WithStore)
            return NAN;

        //! mean value of y
        Type y_mean = Y_[0] / size_;

        // the sum of the squares of the differences between the measured y
        // values and the mean y value
        Type ss_total = 0.0;

        // the sum of the squares of the difference between the measured y
        // values and the values of y predicted by the polynomial
        Type ss_error = 0.0;

        for (const Point& p : points_) {
            ss_total += (p.y - y_mean) * (p.y - y_mean);
            Type y = evaluate(p.x);
            ss_error += (p.y - y) * (p.y - y);
        }

        // 1 - (residual sum of squares / total sum of squares)
        return 1.0 - ss_error / ss_total;
    }

    //! returns value of y predicted by the polynomial for a given value of x
    Type evaluate(const Type& x) {
        return coefficients().evaluate(x);
    }

    //! return coefficients vector
    const Coefficients& coefficients() {
        if (coefficients_.empty()) {
            fit_coefficients();
        }
        return coefficients_;
    }

protected:
    //! polynomial order
    size_t order_;

    //! list of stored points if WithStore, else empty.
    std::vector<Point> points_;

    //! number of points added
    size_t size_;

    //! X_ = vector that stores values of sigma(x_i^2n)
    std::vector<Type> X_;

    //! Y_ = vector to store values of sigma(x_i^order * y_i)
    std::vector<Type> Y_;

    //! cached coefficients
    Coefficients coefficients_;

    //! polynomial regression by inverting a Vandermonde matrix.
    void fit_coefficients() {

        coefficients_.clear();

        if (size_ == 0)
            return;

        size_t np1 = order_ + 1, np2 = order_ + 2;

        // B = normal augmented matrix that stores the equations.
        std::vector<Type> B(np1 * np2, 0);

        for (size_t i = 0; i <= order_; ++i) {
            for (size_t j = 0; j <= order_; ++j) {
                B[i * np2 + j] = X_[i + j];
            }
        }

        // load values of Y_ as last column of B
        for (size_t i = 0; i <= order_; ++i) {
            B[i * np2 + np1] = Y_[i];
        }

        // pivotization of the B matrix.
        for (size_t i = 0; i < order_ + 1; ++i) {
            for (size_t k = i + 1; k < order_ + 1; ++k) {
                if (B[i * np2 + i] < B[k * np2 + i]) {
                    for (size_t j = 0; j <= order_ + 1; ++j) {
                        std::swap(B[i * np2 + j], B[k * np2 + j]);
                    }
                }
            }
        }

        // perform Gaussian elimination.
        for (size_t i = 0; i < order_; ++i) {
            for (size_t k = i + 1; k < order_ + 1; ++k) {
                Type t = B[k * np2 + i] / B[i * np2 + i];
                for (size_t j = 0; j <= order_ + 1; ++j) {
                    B[k * np2 + j] -= t * B[i * np2 + j];
                }
            }
        }

        // back substitution to calculate final coefficients.
        coefficients_.resize(np1);
        for (size_t i = order_ + 1; i != 0; ) {
            --i;
            coefficients_[i] = B[i * np2 + order_ + 1];
            for (size_t j = 0; j < order_ + 1; ++j) {
                if (j != i)
                    coefficients_[i] -= B[i * np2 + j] * coefficients_[j];
            }
            coefficients_[i] /= B[i * np2 + i];
        }
    }
};

//! \}

} // namespace tlx

#endif // !TLX_MATH_POLYNOMIAL_REGRESSION_HEADER

/******************************************************************************/
