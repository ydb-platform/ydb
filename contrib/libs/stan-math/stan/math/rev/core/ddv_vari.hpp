#ifndef STAN_MATH_REV_CORE_DDV_VARI_HPP
#define STAN_MATH_REV_CORE_DDV_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_ddv_vari : public vari {
 protected:
  double ad_;
  double bd_;
  vari* cvi_;

 public:
  op_ddv_vari(double f, double a, double b, vari* cvi)
      : vari(f), ad_(a), bd_(b), cvi_(cvi) {}
};

}  // namespace math
}  // namespace stan
#endif
