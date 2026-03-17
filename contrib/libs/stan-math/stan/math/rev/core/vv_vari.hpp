#ifndef STAN_MATH_REV_CORE_VV_VARI_HPP
#define STAN_MATH_REV_CORE_VV_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_vv_vari : public vari {
 protected:
  vari* avi_;
  vari* bvi_;

 public:
  op_vv_vari(double f, vari* avi, vari* bvi) : vari(f), avi_(avi), bvi_(bvi) {}
};

}  // namespace math
}  // namespace stan
#endif
