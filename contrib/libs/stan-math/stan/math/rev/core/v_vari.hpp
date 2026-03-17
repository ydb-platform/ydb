#ifndef STAN_MATH_REV_CORE_V_VARI_HPP
#define STAN_MATH_REV_CORE_V_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_v_vari : public vari {
 protected:
  vari* avi_;

 public:
  op_v_vari(double f, vari* avi) : vari(f), avi_(avi) {}
};

}  // namespace math
}  // namespace stan
#endif
