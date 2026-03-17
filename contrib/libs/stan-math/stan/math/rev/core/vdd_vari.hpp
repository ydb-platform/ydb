#ifndef STAN_MATH_REV_CORE_VDD_VARI_HPP
#define STAN_MATH_REV_CORE_VDD_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_vdd_vari : public vari {
 protected:
  vari* avi_;
  double bd_;
  double cd_;

 public:
  op_vdd_vari(double f, vari* avi, double b, double c)
      : vari(f), avi_(avi), bd_(b), cd_(c) {}
};

}  // namespace math
}  // namespace stan
#endif
