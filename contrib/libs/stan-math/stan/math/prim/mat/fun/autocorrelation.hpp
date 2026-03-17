#ifndef STAN_MATH_PRIM_MAT_FUN_AUTOCORRELATION_HPP
#define STAN_MATH_PRIM_MAT_FUN_AUTOCORRELATION_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/mean.hpp>
#include <unsupported/Eigen/FFT>
#include <complex>
#include <vector>

namespace stan {
namespace math {
namespace internal {
/**
 * Find the optimal next size for the FFT so that
 * a minimum number of zeros are padded.
 */
inline size_t fft_next_good_size(size_t N) {
  if (N <= 2)
    return 2;
  while (true) {
    size_t m = N;
    while ((m % 2) == 0)
      m /= 2;
    while ((m % 3) == 0)
      m /= 3;
    while ((m % 5) == 0)
      m /= 5;
    if (m <= 1)
      return N;
    N++;
  }
}
}  // namespace internal

/**
 * Write autocorrelation estimates for every lag for the specified
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
 * @param ac Autocorrelations.
 * @param fft FFT engine instance.
 */
template <typename T>
void autocorrelation(const std::vector<T>& y, std::vector<T>& ac,
                     Eigen::FFT<T>& fft) {
  using std::complex;
  using std::vector;

  size_t N = y.size();
  size_t M = internal::fft_next_good_size(N);
  size_t Mt2 = 2 * M;

  vector<complex<T> > freqvec;

  // centered_signal = y-mean(y) followed by N zeroes
  vector<T> centered_signal(y);
  centered_signal.insert(centered_signal.end(), Mt2 - N, 0.0);
  T mean = stan::math::mean(y);
  for (size_t i = 0; i < N; i++)
    centered_signal[i] -= mean;

  fft.fwd(freqvec, centered_signal);
  for (size_t i = 0; i < Mt2; ++i)
    freqvec[i] = complex<T>(norm(freqvec[i]), 0.0);

  fft.inv(ac, freqvec);
  ac.resize(N);

  for (size_t i = 0; i < N; ++i) {
    ac[i] /= (N - i);
  }
  T var = ac[0];
  for (size_t i = 0; i < N; ++i)
    ac[i] /= var;
}

/**
 * Write autocorrelation estimates for every lag for the specified
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
 * @param ac Autocorrelations.
 * @param fft FFT engine instance.
 */
template <typename T, typename DerivedA, typename DerivedB>
void autocorrelation(const Eigen::MatrixBase<DerivedA>& y,
                     Eigen::MatrixBase<DerivedB>& ac, Eigen::FFT<T>& fft) {
  size_t N = y.size();
  size_t M = internal::fft_next_good_size(N);
  size_t Mt2 = 2 * M;

  // centered_signal = y-mean(y) followed by N zeros
  Eigen::Matrix<T, Eigen::Dynamic, 1> centered_signal(Mt2);
  centered_signal.setZero();
  centered_signal.head(N) = y.array() - y.mean();

  Eigen::Matrix<std::complex<T>, Eigen::Dynamic, 1> freqvec(Mt2);
  fft.fwd(freqvec, centered_signal);
  // cwiseAbs2 == norm
  freqvec = freqvec.cwiseAbs2();

  Eigen::Matrix<std::complex<T>, Eigen::Dynamic, 1> ac_tmp(Mt2);
  fft.inv(ac_tmp, freqvec);

  for (size_t i = 0; i < N; ++i)
    ac_tmp(i) /= (N - i);

  ac = ac_tmp.head(N).real().array() / ac_tmp(0).real();
}

/**
 * Write autocorrelation estimates for every lag for the specified
 * input sequence into the specified result.  The return vector be
 * resized to the same length as the input sequence with lags
 * given by array index.
 *
 * <p>The implementation involves a fast Fourier transform,
 * followed by a normalization, followed by an inverse transform.
 *
 * <p>This method is just a light wrapper around the three-argument
 * autocorrelation function.
 *
 * @tparam T Scalar type.
 * @param y Input sequence.
 * @param ac Autocorrelations.
 */
template <typename T>
void autocorrelation(const std::vector<T>& y, std::vector<T>& ac) {
  Eigen::FFT<T> fft;
  size_t N = y.size();
  ac.resize(N);

  const Eigen::Map<const Eigen::Matrix<T, Eigen::Dynamic, 1> > y_map(&y[0], N);
  Eigen::Map<Eigen::Matrix<T, Eigen::Dynamic, 1> > ac_map(&ac[0], N);
  autocorrelation<T>(y_map, ac_map, fft);
}

/**
 * Write autocorrelation estimates for every lag for the specified
 * input sequence into the specified result.  The return vector be
 * resized to the same length as the input sequence with lags
 * given by array index.
 *
 * <p>The implementation involves a fast Fourier transform,
 * followed by a normalization, followed by an inverse transform.
 *
 * <p>This method is just a light wrapper around the three-argument
 * autocorrelation function
 *
 * @tparam T Scalar type.
 * @param y Input sequence.
 * @param ac Autocorrelations.
 */
template <typename T, typename DerivedA, typename DerivedB>
void autocorrelation(const Eigen::MatrixBase<DerivedA>& y,
                     Eigen::MatrixBase<DerivedB>& ac) {
  Eigen::FFT<T> fft;
  autocorrelation(y, ac, fft);
}

}  // namespace math
}  // namespace stan
#endif
