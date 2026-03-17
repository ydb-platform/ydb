#ifndef STAN_MATH_PRIM_MAT_FUN_AUTOCOVARIANCE_HPP
#define STAN_MATH_PRIM_MAT_FUN_AUTOCOVARIANCE_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/autocorrelation.hpp>
#include <stan/math/prim/mat/fun/variance.hpp>
#include <vector>

namespace stan {
namespace math {

/**
 * Write autocovariance estimates for every lag for the specified
 * input sequence into the specified result using the specified
 * FFT engine.  The return vector be resized to the same length as
 * the input sequence with lags given by array index.
 *
 * <p>The implementation involves a fast Fourier transform,
 * followed by a normalization, followed by an inverse transform.
 *
 * <p>An FFT engine can be created for reuse for type double with:
 *
 * <pre>
 *     Eigen::FFT<double> fft;
 * </pre>
 *
 * @tparam T Scalar type.
 * @param y Input sequence.
 * @param acov Autocovariance.
 * @param fft FFT engine instance.
 */
template <typename T>
void autocovariance(const std::vector<T>& y, std::vector<T>& acov,
                    Eigen::FFT<T>& fft) {
  autocorrelation(y, acov, fft);

  T var = variance(y) * (y.size() - 1) / y.size();
  for (size_t i = 0; i < y.size(); i++) {
    acov[i] *= var;
  }
}

/**
 * Write autocovariance estimates for every lag for the specified
 * input sequence into the specified result using the specified
 * FFT engine.  The return vector be resized to the same length as
 * the input sequence with lags given by array index.
 *
 * <p>The implementation involves a fast Fourier transform,
 * followed by a normalization, followed by an inverse transform.
 *
 * <p>An FFT engine can be created for reuse for type double with:
 *
 * <pre>
 *     Eigen::FFT<double> fft;
 * </pre>
 *
 * @tparam T Scalar type.
 * @param y Input sequence.
 * @param acov Autocovariance.
 * @param fft FFT engine instance.
 */
template <typename T, typename DerivedA, typename DerivedB>
void autocovariance(const Eigen::MatrixBase<DerivedA>& y,
                    Eigen::MatrixBase<DerivedB>& acov, Eigen::FFT<T>& fft) {
  autocorrelation(y, acov, fft);
  acov = acov.array() * (y.array() - y.mean()).square().sum() / y.size();
}

/**
 * Write autocovariance estimates for every lag for the specified
 * input sequence into the specified result.  The return vector be
 * resized to the same length as the input sequence with lags
 * given by array index.
 *
 * <p>The implementation involves a fast Fourier transform,
 * followed by a normalization, followed by an inverse transform.
 *
 * <p>This method is just a light wrapper around the three-argument
 * autocovariance function.
 *
 * @tparam T Scalar type.
 * @param y Input sequence.
 * @param acov Autocovariances.
 */
template <typename T>
void autocovariance(const std::vector<T>& y, std::vector<T>& acov) {
  Eigen::FFT<T> fft;
  size_t N = y.size();
  acov.resize(N);

  const Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1> > y_map(&y[0], N);
  Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1> > acov_map(&acov[0], N);
  autocovariance(y_map, acov_map, fft);
}

/**
 * Write autocovariance estimates for every lag for the specified
 * input sequence into the specified result.  The return vector be
 * resized to the same length as the input sequence with lags
 * given by array index.
 *
 * <p>The implementation involves a fast Fourier transform,
 * followed by a normalization, followed by an inverse transform.
 *
 * <p>This method is just a light wrapper around the three-argument
 * autocovariance function
 *
 * @tparam T Scalar type.
 * @param y Input sequence.
 * @param acov Autocovariances.
 */
template <typename T, typename DerivedA, typename DerivedB>
void autocovariance(const Eigen::MatrixBase<DerivedA>& y,
                    Eigen::MatrixBase<DerivedB>& acov) {
  Eigen::FFT<T> fft;
  autocovariance(y, acov, fft);
}

}  // namespace math
}  // namespace stan
#endif
