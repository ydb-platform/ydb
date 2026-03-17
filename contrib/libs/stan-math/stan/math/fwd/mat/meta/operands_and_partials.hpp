#ifndef STAN_MATH_FWD_MAT_META_OPERANDS_AND_PARTIALS_HPP
#define STAN_MATH_FWD_MAT_META_OPERANDS_AND_PARTIALS_HPP

#include <stan/math/fwd/mat/fun/typedefs.hpp>
#include <stan/math/fwd/scal/meta/operands_and_partials.hpp>
#include <stan/math/prim/scal/meta/broadcast_array.hpp>
#include <vector>

namespace stan {
namespace math {
namespace internal {
// Vectorized Univariate
template <typename Dx>
class ops_partials_edge<Dx, std::vector<fvar<Dx> > > {
 public:
  typedef std::vector<fvar<Dx> > Op;
  typedef Eigen::Matrix<Dx, -1, 1> partials_t;
  partials_t partials_;                       // For univariate use-cases
  broadcast_array<partials_t> partials_vec_;  // For multivariate
  explicit ops_partials_edge(const Op& ops)
      : partials_(partials_t::Zero(ops.size())),
        partials_vec_(partials_),
        operands_(ops) {}

 private:
  template <typename, typename, typename, typename, typename, typename>
  friend class stan::math::operands_and_partials;
  const Op& operands_;

  Dx dx() {
    Dx derivative(0);
    for (size_t i = 0; i < this->operands_.size(); ++i) {
      derivative += this->partials_[i] * this->operands_[i].d_;
    }
    return derivative;
  }
};

template <typename Dx, int R, int C>
class ops_partials_edge<Dx, Eigen::Matrix<fvar<Dx>, R, C> > {
 public:
  typedef Eigen::Matrix<Dx, R, C> partials_t;
  typedef Eigen::Matrix<fvar<Dx>, R, C> Op;
  partials_t partials_;                       // For univariate use-cases
  broadcast_array<partials_t> partials_vec_;  // For multivariate
  explicit ops_partials_edge(const Op& ops)
      : partials_(partials_t::Zero(ops.rows(), ops.cols())),
        partials_vec_(partials_),
        operands_(ops) {}

 private:
  template <typename, typename, typename, typename, typename, typename>
  friend class stan::math::operands_and_partials;
  const Op& operands_;

  Dx dx() {
    Dx derivative(0);
    for (int i = 0; i < this->operands_.size(); ++i) {
      derivative += this->partials_(i) * this->operands_(i).d_;
    }
    return derivative;
  }
};

// Multivariate; vectors of eigen types
template <typename Dx, int R, int C>
class ops_partials_edge<Dx, std::vector<Eigen::Matrix<fvar<Dx>, R, C> > > {
 public:
  typedef std::vector<Eigen::Matrix<fvar<Dx>, R, C> > Op;
  typedef Eigen::Matrix<Dx, -1, -1> partial_t;
  std::vector<partial_t> partials_vec_;
  explicit ops_partials_edge(const Op& ops)
      : partials_vec_(ops.size()), operands_(ops) {
    for (size_t i = 0; i < ops.size(); ++i) {
      partials_vec_[i] = partial_t::Zero(ops[i].rows(), ops[i].cols());
    }
  }

 private:
  template <typename, typename, typename, typename, typename, typename>
  friend class stan::math::operands_and_partials;
  const Op& operands_;

  Dx dx() {
    Dx derivative(0);
    for (size_t i = 0; i < this->operands_.size(); ++i) {
      for (int j = 0; j < this->operands_[i].size(); ++j) {
        derivative += this->partials_vec_[i](j) * this->operands_[i](j).d_;
      }
    }
    return derivative;
  }
};

template <typename Dx>
class ops_partials_edge<Dx, std::vector<std::vector<fvar<Dx> > > > {
 public:
  typedef std::vector<std::vector<fvar<Dx> > > Op;
  typedef std::vector<Dx> partial_t;
  std::vector<partial_t> partials_vec_;
  explicit ops_partials_edge(const Op& ops)
      : partials_vec_(length(ops)), operands_(ops) {
    for (size_t i = 0; i < length(ops); ++i) {
      partials_vec_[i] = partial_t(length(ops[i]), 0.0);
    }
  }

 private:
  template <typename, typename, typename, typename, typename, typename>
  friend class stan::math::operands_and_partials;
  const Op& operands_;

  Dx dx() {
    Dx derivative(0);
    for (size_t i = 0; i < this->operands_.size(); ++i) {
      for (size_t j = 0; j < this->operands_[i].size(); ++j) {
        derivative += this->partials_vec_[i][j] * this->operands_[i][j].d_;
      }
    }
    return derivative;
  }
};
}  // namespace internal
}  // namespace math
}  // namespace stan
#endif
