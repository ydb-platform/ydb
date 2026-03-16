#ifndef STAN_MATH_REV_CORE_STORED_GRADIENT_VARI_HPP
#define STAN_MATH_REV_CORE_STORED_GRADIENT_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

/**
 * A var implementation that stores the daughter variable
 * implementation pointers and the partial derivative with respect
 * to the result explicitly in arrays constructed on the
 * auto-diff memory stack.
 *
 * Like a simplified version of OperandsAndPartials.
 */
class stored_gradient_vari : public vari {
 protected:
  size_t size_;
  vari** dtrs_;
  double* partials_;

 public:
  /**
   * Construct a stored gradient vari with the specified
   * value, size, daughter varis, and partial derivatives.
   *
   * @param[in] value Value of vari
   * @param[in] size Number of daughters
   * @param[in] dtrs Array of pointers to daughters
   * @param[in] partials Partial derivatives of value with respect
   * to daughters.
   */
  stored_gradient_vari(double value, size_t size, vari** dtrs, double* partials)
      : vari(value), size_(size), dtrs_(dtrs), partials_(partials) {}

  /**
   * Propagate derivatives through this vari with partial
   * derivatives given for the daughter vari by the stored partials.
   */
  void chain() {
    for (size_t i = 0; i < size_; ++i)
      dtrs_[i]->adj_ += adj_ * partials_[i];
  }
};

}  // namespace math
}  // namespace stan

#endif
