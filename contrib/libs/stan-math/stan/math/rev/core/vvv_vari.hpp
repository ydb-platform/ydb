#ifndef STAN_MATH_REV_CORE_VVV_VARI_HPP
#define STAN_MATH_REV_CORE_VVV_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_vvv_vari : public vari {
 protected:
  vari* avi_;
  vari* bvi_;
  vari* cvi_;

 public:
  op_vvv_vari(double f, vari* avi, vari* bvi, vari* cvi)
      : vari(f), avi_(avi), bvi_(bvi), cvi_(cvi) {}
};

}  // namespace math
}  // namespace stan
#endif
