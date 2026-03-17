#ifndef STAN_MATH_REV_CORE_DV_VARI_HPP
#define STAN_MATH_REV_CORE_DV_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_dv_vari : public vari {
 protected:
  double ad_;
  vari* bvi_;

 public:
  op_dv_vari(double f, double a, vari* bvi) : vari(f), ad_(a), bvi_(bvi) {}
};

}  // namespace math
}  // namespace stan
#endif
