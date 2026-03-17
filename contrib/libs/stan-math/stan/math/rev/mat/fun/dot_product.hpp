#ifndef STAN_MATH_REV_MAT_FUN_DOT_PRODUCT_HPP
#define STAN_MATH_REV_MAT_FUN_DOT_PRODUCT_HPP

#include <stan/math/prim/mat/fun/Eigen.hpp>
#include <stan/math/prim/mat/fun/typedefs.hpp>
#include <stan/math/prim/mat/err/check_vector.hpp>
#include <stan/math/prim/arr/err/check_matching_sizes.hpp>
#include <stan/math/prim/scal/fun/value_of.hpp>
#include <stan/math/rev/core.hpp>
#include <stan/math/rev/mat/fun/typedefs.hpp>
#include <stan/math/rev/scal/fun/value_of.hpp>
#include <type_traits>
#include <vector>

namespace stan {
namespace math {

namespace internal {
template <typename T>
struct dot_product_store_type;

template <>
struct dot_product_store_type<var> {
  typedef vari** type;
};

template <>
struct dot_product_store_type<double> {
  typedef double* type;
};

template <typename T1, typename T2>
class dot_product_vari : public vari {
 protected:
  typename dot_product_store_type<T1>::type v1_;
  typename dot_product_store_type<T2>::type v2_;
  size_t length_;

  inline static double var_dot(vari** v1, vari** v2, size_t length) {
    Eigen::VectorXd vd1(length), vd2(length);
    for (size_t i = 0; i < length; i++) {
      vd1[i] = v1[i]->val_;
      vd2[i] = v2[i]->val_;
    }
    return vd1.dot(vd2);
  }

  inline static double var_dot(const T1* v1, const T2* v2, size_t length) {
    Eigen::VectorXd vd1(length), vd2(length);
    for (size_t i = 0; i < length; i++) {
      vd1[i] = value_of(v1[i]);
      vd2[i] = value_of(v2[i]);
    }
    return vd1.dot(vd2);
  }

  template <typename Derived1, typename Derived2>
  inline static double var_dot(const Eigen::DenseBase<Derived1>& v1,
                               const Eigen::DenseBase<Derived2>& v2) {
    Eigen::VectorXd vd1(v1.size()), vd2(v1.size());
    for (int i = 0; i < v1.size(); i++) {
      vd1[i] = value_of(v1[i]);
      vd2[i] = value_of(v2[i]);
    }
    return vd1.dot(vd2);
  }
  inline void chain(vari** v1, vari** v2) {
    for (size_t i = 0; i < length_; i++) {
      v1[i]->adj_ += adj_ * v2_[i]->val_;
      v2[i]->adj_ += adj_ * v1_[i]->val_;
    }
  }
  inline void chain(double* v1, vari** v2) {
    for (size_t i = 0; i < length_; i++) {
      v2[i]->adj_ += adj_ * v1_[i];
    }
  }
  inline void chain(vari** v1, double* v2) {
    for (size_t i = 0; i < length_; i++) {
      v1[i]->adj_ += adj_ * v2_[i];
    }
  }
  inline void initialize(vari**& mem_v, const var* inv,
                         vari** shared = nullptr) {
    if (shared == nullptr) {
      mem_v = reinterpret_cast<vari**>(
          ChainableStack::instance().memalloc_.alloc(length_ * sizeof(vari*)));
      for (size_t i = 0; i < length_; i++)
        mem_v[i] = inv[i].vi_;
    } else {
      mem_v = shared;
    }
  }
  template <typename Derived>
  inline void initialize(vari**& mem_v, const Eigen::DenseBase<Derived>& inv,
                         vari** shared = nullptr) {
    if (shared == nullptr) {
      mem_v = reinterpret_cast<vari**>(
          ChainableStack::instance().memalloc_.alloc(length_ * sizeof(vari*)));
      for (size_t i = 0; i < length_; i++)
        mem_v[i] = inv(i).vi_;
    } else {
      mem_v = shared;
    }
  }

  inline void initialize(double*& mem_d, const double* ind,
                         double* shared = nullptr) {
    if (shared == nullptr) {
      mem_d = reinterpret_cast<double*>(
          ChainableStack::instance().memalloc_.alloc(length_ * sizeof(double)));
      for (size_t i = 0; i < length_; i++)
        mem_d[i] = ind[i];
    } else {
      mem_d = shared;
    }
  }
  template <typename Derived>
  inline void initialize(double*& mem_d, const Eigen::DenseBase<Derived>& ind,
                         double* shared = nullptr) {
    if (shared == nullptr) {
      mem_d = reinterpret_cast<double*>(
          ChainableStack::instance().memalloc_.alloc(length_ * sizeof(double)));
      for (size_t i = 0; i < length_; i++)
        mem_d[i] = ind(i);
    } else {
      mem_d = shared;
    }
  }

 public:
  dot_product_vari(typename dot_product_store_type<T1>::type v1,
                   typename dot_product_store_type<T2>::type v2, size_t length)
      : vari(var_dot(v1, v2, length)), v1_(v1), v2_(v2), length_(length) {}

  dot_product_vari(const T1* v1, const T2* v2, size_t length,
                   dot_product_vari<T1, T2>* shared_v1 = NULL,
                   dot_product_vari<T1, T2>* shared_v2 = NULL)
      : vari(var_dot(v1, v2, length)), length_(length) {
    if (shared_v1 == NULL) {
      initialize(v1_, v1);
    } else {
      initialize(v1_, v1, shared_v1->v1_);
    }
    if (shared_v2 == NULL) {
      initialize(v2_, v2);
    } else {
      initialize(v2_, v2, shared_v2->v2_);
    }
  }
  template <typename Derived1, typename Derived2>
  dot_product_vari(const Eigen::DenseBase<Derived1>& v1,
                   const Eigen::DenseBase<Derived2>& v2,
                   dot_product_vari<T1, T2>* shared_v1 = NULL,
                   dot_product_vari<T1, T2>* shared_v2 = NULL)
      : vari(var_dot(v1, v2)), length_(v1.size()) {
    if (shared_v1 == NULL) {
      initialize(v1_, v1);
    } else {
      initialize(v1_, v1, shared_v1->v1_);
    }
    if (shared_v2 == NULL) {
      initialize(v2_, v2);
    } else {
      initialize(v2_, v2, shared_v2->v2_);
    }
  }
  template <int R1, int C1, int R2, int C2>
  dot_product_vari(const Eigen::Matrix<T1, R1, C1>& v1,
                   const Eigen::Matrix<T2, R2, C2>& v2,
                   dot_product_vari<T1, T2>* shared_v1 = NULL,
                   dot_product_vari<T1, T2>* shared_v2 = NULL)
      : vari(var_dot(v1, v2)), length_(v1.size()) {
    if (shared_v1 == NULL) {
      initialize(v1_, v1);
    } else {
      initialize(v1_, v1, shared_v1->v1_);
    }
    if (shared_v2 == NULL) {
      initialize(v2_, v2);
    } else {
      initialize(v2_, v2, shared_v2->v2_);
    }
  }
  virtual void chain() { chain(v1_, v2_); }
};
}  // namespace internal

/**
 * Returns the dot product.
 *
 * @param[in] v1 First column vector.
 * @param[in] v2 Second column vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error if length of v1 is not equal to length of v2.
 */
template <typename T1, int R1, int C1, typename T2, int R2, int C2>
inline typename std::enable_if<
    std::is_same<T1, var>::value || std::is_same<T2, var>::value, var>::type
dot_product(const Eigen::Matrix<T1, R1, C1>& v1,
            const Eigen::Matrix<T2, R2, C2>& v2) {
  check_vector("dot_product", "v1", v1);
  check_vector("dot_product", "v2", v2);
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);
  return var(new internal::dot_product_vari<T1, T2>(v1, v2));
}
/**
 * Returns the dot product.
 *
 * @param[in] v1 First array.
 * @param[in] v2 Second array.
 * @param[in] length Length of both arrays.
 * @return Dot product of the arrays.
 */
template <typename T1, typename T2>
inline typename std::enable_if<
    std::is_same<T1, var>::value || std::is_same<T2, var>::value, var>::type
dot_product(const T1* v1, const T2* v2, size_t length) {
  return var(new internal::dot_product_vari<T1, T2>(v1, v2, length));
}

/**
 * Returns the dot product.
 *
 * @param[in] v1 First vector.
 * @param[in] v2 Second vector.
 * @return Dot product of the vectors.
 * @throw std::domain_error if sizes of v1 and v2 do not match.
 */
template <typename T1, typename T2>
inline typename std::enable_if<
    std::is_same<T1, var>::value || std::is_same<T2, var>::value, var>::type
dot_product(const std::vector<T1>& v1, const std::vector<T2>& v2) {
  check_matching_sizes("dot_product", "v1", v1, "v2", v2);
  return var(new internal::dot_product_vari<T1, T2>(&v1[0], &v2[0], v1.size()));
}

}  // namespace math
}  // namespace stan
#endif
