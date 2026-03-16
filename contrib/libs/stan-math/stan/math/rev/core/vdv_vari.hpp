#ifndef STAN_MATH_REV_CORE_VDV_VARI_HPP
#define STAN_MATH_REV_CORE_VDV_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_vdv_vari : public vari {
 protected:
  vari* avi_;
  double bd_;
  vari* cvi_;

 public:
  op_vdv_vari(double f, vari* avi, double b, vari* cvi)
      : vari(f), avi_(avi), bd_(b), cvi_(cvi) {}
};

}  // namespace math
}  // namespace stan
#endif
