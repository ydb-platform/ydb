#ifndef STAN_MATH_REV_CORE_VD_VARI_HPP
#define STAN_MATH_REV_CORE_VD_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_vd_vari : public vari {
 protected:
  vari* avi_;
  double bd_;

 public:
  op_vd_vari(double f, vari* avi, double b) : vari(f), avi_(avi), bd_(b) {}
};

}  // namespace math
}  // namespace stan
#endif
