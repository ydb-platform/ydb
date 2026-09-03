/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifndef POW_HPP
#define POW_HPP

/*
 * An efficient computation of exponentiation to an INTEGER power.
 * See, Leonid's blog entry for details:
 *
 * http://searchivarius.org/blog/how-fast-are-our-math-libraries 
 *
 */

#include <cmath>
#include <limits>

#include "logging.h"

namespace similarity {

template <class T>
inline T EfficientPow(T Base, unsigned Exp) {
    if (Exp == 0) return 1;
    if (Exp == 1) return Base; 
    if (Exp == 2) return Base * Base; 
    if (Exp == 3) return Base * Base * Base; 
    if (Exp == 4) {
        Base *= Base;
        return Base * Base;
    }

    T res = Base;

    if (Exp == 5) {
        Base *= Base;
        return res * Base * Base;
    }

    if (Exp == 6) {
        Base *= Base;
        res = Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 7) {
        Base *= Base;
        res *= Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 8) {
        Base *= Base;
        Base *= Base;
        Base *= Base;
        return Base;
    }

    if (Exp == 9) {
        Base *= Base;
        Base *= Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 10) {
        Base *= Base;
        res = Base;
        Base *= Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 11) {
        Base *= Base;
        res  *= Base;
        Base *= Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 12) {
        Base *= Base;
        Base *= Base;
        res = Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 13) {
        Base *= Base;
        Base *= Base;
        res *= Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 14) {
        Base *= Base;
        res = Base;
        Base *= Base;
        res *= Base;
        Base *= Base;
        return res * Base;
    }

    if (Exp == 15) {
        Base *= Base;
        res *= Base;
        Base *= Base;
        res *= Base;
        Base *= Base;
        return res * Base;
    }

    res *= res;
    res *= res;
    res *= res;
    res *= res;

    if (Exp == 16) {
        return res;
    }

    Exp -= 16;

    while (true) {
        if (Exp & 1) res *= Base;
        Exp >>= 1;
        if (Exp) {
            Base *= Base;
        } else {
            return res;
        }
    }

    return res;
}


template <class T>
inline T EfficientFractPowUtil(T Base, uint64_t Exp, uint64_t MaxK) {
    if (Exp == 0)    return 1;     // pow == 0 
    if (Exp == MaxK) return Base;  // pow == 1

    uint64_t Mask1 = MaxK - 1;
    uint64_t Mask2 = MaxK >>= 1;

    T res = 1.0;

    while (true) {
        Base = sqrt(Base);

        if (Exp & Mask2) res *= Base;

        Exp = (Exp << 1) & Mask1;

        if (!Exp) return res;
    }

    return res;
}

template <class T>
inline T EfficientFractPow(T Base, T FractExp, unsigned NumDig) {
    CHECK(FractExp <= 1 && NumDig);
    uint64_t MaxK = uint64_t(1) << NumDig;
    uint64_t Exp = static_cast<unsigned>(std::ceil(FractExp * MaxK));

    return EfficientFractPowUtil(Base, Exp, MaxK);
}

/**
  * A helper object that does some preprocessing for subsequent
  * efficient computation of both integer and fractional
  * powers for exponents x, where x * 2^maxDig is an integer.
  * In other words, this can be done for exponents that have
  * zeros beyond maxDig binary digits after the binary point.
  * When the exponent does not satisfy this property,
  * we simply default to using the standard std::pow function.
  */
template <class T>
class PowerProxyObject {
public:
  /**
    * Constructor.
    *
    * @param  p       is an exponent
    * @param  maxDig  a maximum number of binary digits to consider (should be <= 31).
    */
  PowerProxyObject(const T p, const unsigned maxDig = 18) {
    pOrig_ = p;
    isNeg_ = p < 0;
    p_ = (isNeg_) ? -p : p;
    maxK_ = 1u << maxDig;
    unsigned pfm = static_cast<unsigned>(std::floor(maxK_ * p_));

    isOptim_   = (fabs(maxK_*p_ - pfm) <= 2 * std::numeric_limits<T>::min());
    intPow_    = pfm >> maxDig;
    fractPow_  = pfm - (intPow_ << maxDig);

  }
  
  /**
    * Compute pow(base, p_) possibly efficiently. We expect base to be 
    * non-negative!
    */
  T inline pow(T base) {
    if (isOptim_) {
      if (isNeg_) base = 1/base; // Negative power
      T mult1  = intPow_ ?    EfficientPow(base, intPow_) : 1;
      T mult2  = fractPow_ ?  EfficientFractPowUtil(base, fractPow_, maxK_) : 1;
      return mult1 * mult2;
    } else {
      return std::pow(base, pOrig_);
    }
  }
private:
  bool     isNeg_;
  T        pOrig_;
  T        p_;
  unsigned maxK_;
  bool     isOptim_;
  unsigned intPow_;
  unsigned fractPow_;
};


}

#endif
