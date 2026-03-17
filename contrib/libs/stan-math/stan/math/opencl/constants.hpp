#ifndef STAN_MATH_OPENCL_CONSTANTS_HPP
#define STAN_MATH_OPENCL_CONSTANTS_HPP
#ifdef STAN_OPENCL
namespace stan {
namespace math {
enum class TriangularViewCL { Lower = 0, Upper = 1, Entire = 2 };
enum class TriangularMapCL { UpperToLower = 0, LowerToUpper = 1 };
}  // namespace math
}  // namespace stan
#endif
#endif
