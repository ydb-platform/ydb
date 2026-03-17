#ifndef STAN_MATH_REV_CORE_DVD_VARI_HPP
#define STAN_MATH_REV_CORE_DVD_VARI_HPP

#include <stan/math/rev/core/vari.hpp>

namespace stan {
namespace math {

class op_dvd_vari : public vari {
 protected:
  double ad_;
  vari* bvi_;
  double cd_;

 public:
  op_dvd_vari(double f, double a, vari* bvi, double c)
      : vari(f), ad_(a), bvi_(bvi), cd_(c) {}
};

}  // namespace math
}  // namespace stan
#endif
