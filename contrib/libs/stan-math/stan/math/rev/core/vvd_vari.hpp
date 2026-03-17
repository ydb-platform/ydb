#ifndef STAN_MATH_REV_CORE_VVD_VARI_HPP
#define STAN_MATH_REV_CORE_VVD_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_vvd_vari : public vari {
 protected:
  vari* avi_;
  vari* bvi_;
  double cd_;

 public:
  op_vvd_vari(double f, vari* avi, vari* bvi, double c)
      : vari(f), avi_(avi), bvi_(bvi), cd_(c) {}
};

}  // namespace math
}  // namespace stan
#endif
