// This file is MACHINE GENERATED! Do not edit.

#ifndef TENSORFLOW_CC_OPS_LINALG_OPS_H_
#define TENSORFLOW_CC_OPS_LINALG_OPS_H_

// This file is MACHINE GENERATED! Do not edit.

#include "tensorflow/cc/framework/ops.h"
#include "tensorflow/cc/framework/scope.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/lib/gtl/array_slice.h"

namespace tensorflow {
namespace ops {

/// @defgroup linalg_ops Linalg Ops
/// @{

/// Computes the Cholesky decomposition of one or more square matrices.
///
/// The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
/// form square matrices.
///
/// The input has to be symmetric and positive definite. Only the lower-triangular
/// part of the input will be used for this operation. The upper-triangular part
/// will not be read.
///
/// The output is a tensor of the same shape as the input
/// containing the Cholesky decompositions for all input submatrices `[..., :, :]`.
///
/// **Note**: The gradient computation on GPU is faster for large matrices but
/// not for large batch dimensions when the submatrices are small. In this
/// case it might be faster to use the CPU.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Shape is `[..., M, M]`.
///
/// Returns:
/// * `Output`: Shape is `[..., M, M]`.
class Cholesky {
 public:
  Cholesky(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the reverse mode backpropagated gradient of the Cholesky algorithm.
///
/// For an explanation see "Differentiation of the Cholesky algorithm" by
/// Iain Murray http://arxiv.org/abs/1602.07527.
///
/// Arguments:
/// * scope: A Scope object
/// * l: Output of batch Cholesky algorithm l = cholesky(A). Shape is `[..., M, M]`.
/// Algorithm depends only on lower triangular part of the innermost matrices of
/// this tensor.
/// * grad: df/dl where f is some scalar function. Shape is `[..., M, M]`.
/// Algorithm depends only on lower triangular part of the innermost matrices of
/// this tensor.
///
/// Returns:
/// * `Output`: Symmetrized version of df/dA . Shape is `[..., M, M]`
class CholeskyGrad {
 public:
  CholeskyGrad(const ::tensorflow::Scope& scope, ::tensorflow::Input l,
             ::tensorflow::Input grad);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the sign and the log of the absolute value of the determinant of
///
/// one or more square matrices.
///
/// The input is a tensor of shape `[N, M, M]` whose inner-most 2 dimensions
/// form square matrices. The outputs are two tensors containing the signs and
/// absolute values of the log determinants for all N input submatrices
/// `[..., :, :]` such that the determinant = sign*exp(log_abs_determinant).
/// The log_abs_determinant is computed as det(P)*sum(log(diag(LU))) where LU
/// is the LU decomposition of the input and P is the corresponding
/// permutation matrix.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Shape is `[N, M, M]`.
///
/// Returns:
/// * `Output` sign: The signs of the log determinants of the inputs. Shape is `[N]`.
/// * `Output` log_abs_determinant: The logs of the absolute values of the determinants
/// of the N input matrices.  Shape is `[N]`.
class LogMatrixDeterminant {
 public:
  LogMatrixDeterminant(const ::tensorflow::Scope& scope, ::tensorflow::Input
                     input);

  ::tensorflow::Output sign;
  ::tensorflow::Output log_abs_determinant;
};

/// Computes the determinant of one or more square matrices.
///
/// The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
/// form square matrices. The output is a tensor containing the determinants
/// for all input submatrices `[..., :, :]`.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Shape is `[..., M, M]`.
///
/// Returns:
/// * `Output`: Shape is `[...]`.
class MatrixDeterminant {
 public:
  MatrixDeterminant(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the matrix exponential of one or more square matrices:
///
/// \\(exp(A) = \sum_{n=0}^\infty A^n/n!\\)
///
/// The exponential is computed using a combination of the scaling and squaring
/// method and the Pade approximation. Details can be founds in:
/// Nicholas J. Higham, "The scaling and squaring method for the matrix exponential
/// revisited," SIAM J. Matrix Anal. Applic., 26:1179-1193, 2005.
///
/// The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
/// form square matrices. The output is a tensor of the same shape as the input
/// containing the exponential for all input submatrices `[..., :, :]`.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Shape is `[..., M, M]`.
///
/// Returns:
/// * `Output`: Shape is `[..., M, M]`.
///
/// @compatibility(scipy)
/// Equivalent to scipy.linalg.expm
/// @end_compatibility
class MatrixExponential {
 public:
  MatrixExponential(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  ::tensorflow::Output output;
};

/// Computes the inverse of one or more square invertible matrices or their
///
/// adjoints (conjugate transposes).
///
/// The input is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
/// form square matrices. The output is a tensor of the same shape as the input
/// containing the inverse for all input submatrices `[..., :, :]`.
///
/// The op uses LU decomposition with partial pivoting to compute the inverses.
///
/// If a matrix is not invertible there is no guarantee what the op does. It
/// may detect the condition and raise an exception or it may simply return a
/// garbage result.
///
/// Arguments:
/// * scope: A Scope object
/// * input: Shape is `[..., M, M]`.
///
/// Returns:
/// * `Output`: Shape is `[..., M, M]`.
///
/// @compatibility(numpy)
/// Equivalent to np.linalg.inv
/// @end_compatibility
class MatrixInverse {
 public:
  /// Optional attribute setters for MatrixInverse
  struct Attrs {
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Adjoint(bool x) {
      Attrs ret = *this;
      ret.adjoint_ = x;
      return ret;
    }

    bool adjoint_ = false;
  };
  MatrixInverse(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  MatrixInverse(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
              const MatrixInverse::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Adjoint(bool x) {
    return Attrs().Adjoint(x);
  }

  ::tensorflow::Output output;
};

/// Solves systems of linear equations.
///
/// `Matrix` is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions
/// form square matrices. `Rhs` is a tensor of shape `[..., M, K]`. The `output` is
/// a tensor shape `[..., M, K]`.  If `adjoint` is `False` then each output matrix
/// satisfies `matrix[..., :, :] * output[..., :, :] = rhs[..., :, :]`.
/// If `adjoint` is `True` then each output matrix satisfies
/// `adjoint(matrix[..., :, :]) * output[..., :, :] = rhs[..., :, :]`.
///
/// Arguments:
/// * scope: A Scope object
/// * matrix: Shape is `[..., M, M]`.
/// * rhs: Shape is `[..., M, K]`.
///
/// Optional attributes (see `Attrs`):
/// * adjoint: Boolean indicating whether to solve with `matrix` or its (block-wise)
/// adjoint.
///
/// Returns:
/// * `Output`: Shape is `[..., M, K]`.
class MatrixSolve {
 public:
  /// Optional attribute setters for MatrixSolve
  struct Attrs {
    /// Boolean indicating whether to solve with `matrix` or its (block-wise)
    /// adjoint.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Adjoint(bool x) {
      Attrs ret = *this;
      ret.adjoint_ = x;
      return ret;
    }

    bool adjoint_ = false;
  };
  MatrixSolve(const ::tensorflow::Scope& scope, ::tensorflow::Input matrix,
            ::tensorflow::Input rhs);
  MatrixSolve(const ::tensorflow::Scope& scope, ::tensorflow::Input matrix,
            ::tensorflow::Input rhs, const MatrixSolve::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Adjoint(bool x) {
    return Attrs().Adjoint(x);
  }

  ::tensorflow::Output output;
};

/// Solves one or more linear least-squares problems.
///
/// `matrix` is a tensor of shape `[..., M, N]` whose inner-most 2 dimensions
/// form real or complex matrices of size `[M, N]`. `Rhs` is a tensor of the same
/// type as `matrix` and shape `[..., M, K]`.
/// The output is a tensor shape `[..., N, K]` where each output matrix solves
/// each of the equations
/// `matrix[..., :, :]` * `output[..., :, :]` = `rhs[..., :, :]`
/// in the least squares sense.
///
/// We use the following notation for (complex) matrix and right-hand sides
/// in the batch:
///
/// `matrix`=\\(A \in \mathbb{C}^{m \times n}\\),
/// `rhs`=\\(B  \in \mathbb{C}^{m \times k}\\),
/// `output`=\\(X  \in \mathbb{C}^{n \times k}\\),
/// `l2_regularizer`=\\(\lambda \in \mathbb{R}\\).
///
/// If `fast` is `True`, then the solution is computed by solving the normal
/// equations using Cholesky decomposition. Specifically, if \\(m \ge n\\) then
/// \\(X = (A^H A + \lambda I)^{-1} A^H B\\), which solves the least-squares
/// problem \\(X = \mathrm{argmin}_{Z \in \Re^{n \times k} } ||A Z - B||_F^2 + \lambda ||Z||_F^2\\).
/// If \\(m \lt n\\) then `output` is computed as
/// \\(X = A^H (A A^H + \lambda I)^{-1} B\\), which (for \\(\lambda = 0\\)) is the
/// minimum-norm solution to the under-determined linear system, i.e.
/// \\(X = \mathrm{argmin}_{Z \in \mathbb{C}^{n \times k} } ||Z||_F^2 \\),
/// subject to \\(A Z = B\\). Notice that the fast path is only numerically stable
/// when \\(A\\) is numerically full rank and has a condition number
/// \\(\mathrm{cond}(A) \lt \frac{1}{\sqrt{\epsilon_{mach} } }\\) or \\(\lambda\\) is
/// sufficiently large.
///
/// If `fast` is `False` an algorithm based on the numerically robust complete
/// orthogonal decomposition is used. This computes the minimum-norm
/// least-squares solution, even when \\(A\\) is rank deficient. This path is
/// typically 6-7 times slower than the fast path. If `fast` is `False` then
/// `l2_regularizer` is ignored.
///
/// Arguments:
/// * scope: A Scope object
/// * matrix: Shape is `[..., M, N]`.
/// * rhs: Shape is `[..., M, K]`.
/// * l2_regularizer: Scalar tensor.
///
/// @compatibility(numpy)
/// Equivalent to np.linalg.lstsq
/// @end_compatibility
///
/// Returns:
/// * `Output`: Shape is `[..., N, K]`.
class MatrixSolveLs {
 public:
  /// Optional attribute setters for MatrixSolveLs
  struct Attrs {
    /// Defaults to true
    TF_MUST_USE_RESULT Attrs Fast(bool x) {
      Attrs ret = *this;
      ret.fast_ = x;
      return ret;
    }

    bool fast_ = true;
  };
  MatrixSolveLs(const ::tensorflow::Scope& scope, ::tensorflow::Input matrix,
              ::tensorflow::Input rhs, ::tensorflow::Input l2_regularizer);
  MatrixSolveLs(const ::tensorflow::Scope& scope, ::tensorflow::Input matrix,
              ::tensorflow::Input rhs, ::tensorflow::Input l2_regularizer,
              const MatrixSolveLs::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Fast(bool x) {
    return Attrs().Fast(x);
  }

  ::tensorflow::Output output;
};

/// Solves systems of linear equations with upper or lower triangular matrices by
///
/// backsubstitution.
///
/// `matrix` is a tensor of shape `[..., M, M]` whose inner-most 2 dimensions form
/// square matrices. If `lower` is `True` then the strictly upper triangular part
/// of each inner-most matrix is assumed to be zero and not accessed.
/// If `lower` is False then the strictly lower triangular part of each inner-most
/// matrix is assumed to be zero and not accessed.
/// `rhs` is a tensor of shape `[..., M, K]`.
///
/// The output is a tensor of shape `[..., M, K]`. If `adjoint` is
/// `True` then the innermost matrices in `output` satisfy matrix equations
/// `matrix[..., :, :] * output[..., :, :] = rhs[..., :, :]`.
/// If `adjoint` is `False` then the strictly then the  innermost matrices in
/// `output` satisfy matrix equations
/// `adjoint(matrix[..., i, k]) * output[..., k, j] = rhs[..., i, j]`.
///
/// Arguments:
/// * scope: A Scope object
/// * matrix: Shape is `[..., M, M]`.
/// * rhs: Shape is `[..., M, K]`.
///
/// Optional attributes (see `Attrs`):
/// * lower: Boolean indicating whether the innermost matrices in `matrix` are
/// lower or upper triangular.
/// * adjoint: Boolean indicating whether to solve with `matrix` or its (block-wise)
///          adjoint.
///
/// @compatibility(numpy)
/// Equivalent to scipy.linalg.solve_triangular
/// @end_compatibility
///
/// Returns:
/// * `Output`: Shape is `[..., M, K]`.
class MatrixTriangularSolve {
 public:
  /// Optional attribute setters for MatrixTriangularSolve
  struct Attrs {
    /// Boolean indicating whether the innermost matrices in `matrix` are
    /// lower or upper triangular.
    ///
    /// Defaults to true
    TF_MUST_USE_RESULT Attrs Lower(bool x) {
      Attrs ret = *this;
      ret.lower_ = x;
      return ret;
    }

    /// Boolean indicating whether to solve with `matrix` or its (block-wise)
    ///          adjoint.
    ///
    /// @compatibility(numpy)
    /// Equivalent to scipy.linalg.solve_triangular
    /// @end_compatibility
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs Adjoint(bool x) {
      Attrs ret = *this;
      ret.adjoint_ = x;
      return ret;
    }

    bool lower_ = true;
    bool adjoint_ = false;
  };
  MatrixTriangularSolve(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      matrix, ::tensorflow::Input rhs);
  MatrixTriangularSolve(const ::tensorflow::Scope& scope, ::tensorflow::Input
                      matrix, ::tensorflow::Input rhs, const
                      MatrixTriangularSolve::Attrs& attrs);
  operator ::tensorflow::Output() const { return output; }
  operator ::tensorflow::Input() const { return output; }
  ::tensorflow::Node* node() const { return output.node(); }

  static Attrs Lower(bool x) {
    return Attrs().Lower(x);
  }
  static Attrs Adjoint(bool x) {
    return Attrs().Adjoint(x);
  }

  ::tensorflow::Output output;
};

/// Computes the QR decompositions of one or more matrices.
///
/// Computes the QR decomposition of each inner matrix in `tensor` such that
/// `tensor[..., :, :] = q[..., :, :] * r[..., :,:])`
///
/// ```python
/// # a is a tensor.
/// # q is a tensor of orthonormal matrices.
/// # r is a tensor of upper triangular matrices.
/// q, r = qr(a)
/// q_full, r_full = qr(a, full_matrices=True)
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * input: A tensor of shape `[..., M, N]` whose inner-most 2 dimensions
/// form matrices of size `[M, N]`. Let `P` be the minimum of `M` and `N`.
///
/// Optional attributes (see `Attrs`):
/// * full_matrices: If true, compute full-sized `q` and `r`. If false
/// (the default), compute only the leading `P` columns of `q`.
///
/// Returns:
/// * `Output` q: Orthonormal basis for range of `a`. If `full_matrices` is `False` then
/// shape is `[..., M, P]`; if `full_matrices` is `True` then shape is
/// `[..., M, M]`.
/// * `Output` r: Triangular factor. If `full_matrices` is `False` then shape is
/// `[..., P, N]`. If `full_matrices` is `True` then shape is `[..., M, N]`.
class Qr {
 public:
  /// Optional attribute setters for Qr
  struct Attrs {
    /// If true, compute full-sized `q` and `r`. If false
    /// (the default), compute only the leading `P` columns of `q`.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs FullMatrices(bool x) {
      Attrs ret = *this;
      ret.full_matrices_ = x;
      return ret;
    }

    bool full_matrices_ = false;
  };
  Qr(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  Qr(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
   Qr::Attrs& attrs);

  static Attrs FullMatrices(bool x) {
    return Attrs().FullMatrices(x);
  }

  ::tensorflow::Output q;
  ::tensorflow::Output r;
};

/// Computes the eigen decomposition of one or more square self-adjoint matrices.
///
/// Computes the eigenvalues and (optionally) eigenvectors of each inner matrix in
/// `input` such that `input[..., :, :] = v[..., :, :] * diag(e[..., :])`. The eigenvalues
/// are sorted in non-decreasing order.
///
/// ```python
/// # a is a tensor.
/// # e is a tensor of eigenvalues.
/// # v is a tensor of eigenvectors.
/// e, v = self_adjoint_eig(a)
/// e = self_adjoint_eig(a, compute_v=False)
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * input: `Tensor` input of shape `[N, N]`.
///
/// Optional attributes (see `Attrs`):
/// * compute_v: If `True` then eigenvectors will be computed and returned in `v`.
/// Otherwise, only the eigenvalues will be computed.
///
/// Returns:
/// * `Output` e: Eigenvalues. Shape is `[N]`.
/// * `Output` v: Eigenvectors. Shape is `[N, N]`.
class SelfAdjointEig {
 public:
  /// Optional attribute setters for SelfAdjointEig
  struct Attrs {
    /// If `True` then eigenvectors will be computed and returned in `v`.
    /// Otherwise, only the eigenvalues will be computed.
    ///
    /// Defaults to true
    TF_MUST_USE_RESULT Attrs ComputeV(bool x) {
      Attrs ret = *this;
      ret.compute_v_ = x;
      return ret;
    }

    bool compute_v_ = true;
  };
  SelfAdjointEig(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  SelfAdjointEig(const ::tensorflow::Scope& scope, ::tensorflow::Input input,
               const SelfAdjointEig::Attrs& attrs);

  static Attrs ComputeV(bool x) {
    return Attrs().ComputeV(x);
  }

  ::tensorflow::Output e;
  ::tensorflow::Output v;
};

/// Computes the singular value decompositions of one or more matrices.
///
/// Computes the SVD of each inner matrix in `input` such that
/// `input[..., :, :] = u[..., :, :] * diag(s[..., :, :]) * transpose(v[..., :, :])`
///
/// ```python
/// # a is a tensor containing a batch of matrices.
/// # s is a tensor of singular values for each matrix.
/// # u is the tensor containing of left singular vectors for each matrix.
/// # v is the tensor containing of right singular vectors for each matrix.
/// s, u, v = svd(a)
/// s, _, _ = svd(a, compute_uv=False)
/// ```
///
/// Arguments:
/// * scope: A Scope object
/// * input: A tensor of shape `[..., M, N]` whose inner-most 2 dimensions
/// form matrices of size `[M, N]`. Let `P` be the minimum of `M` and `N`.
///
/// Optional attributes (see `Attrs`):
/// * compute_uv: If true, left and right singular vectors will be
/// computed and returned in `u` and `v`, respectively.
/// If false, `u` and `v` are not set and should never referenced.
/// * full_matrices: If true, compute full-sized `u` and `v`. If false
/// (the default), compute only the leading `P` singular vectors.
/// Ignored if `compute_uv` is `False`.
///
/// Returns:
/// * `Output` s: Singular values. Shape is `[..., P]`.
/// * `Output` u: Left singular vectors. If `full_matrices` is `False` then shape is
/// `[..., M, P]`; if `full_matrices` is `True` then shape is
/// `[..., M, M]`. Undefined if `compute_uv` is `False`.
/// * `Output` v: Left singular vectors. If `full_matrices` is `False` then shape is
/// `[..., N, P]`. If `full_matrices` is `True` then shape is `[..., N, N]`.
/// Undefined if `compute_uv` is false.
class Svd {
 public:
  /// Optional attribute setters for Svd
  struct Attrs {
    /// If true, left and right singular vectors will be
    /// computed and returned in `u` and `v`, respectively.
    /// If false, `u` and `v` are not set and should never referenced.
    ///
    /// Defaults to true
    TF_MUST_USE_RESULT Attrs ComputeUv(bool x) {
      Attrs ret = *this;
      ret.compute_uv_ = x;
      return ret;
    }

    /// If true, compute full-sized `u` and `v`. If false
    /// (the default), compute only the leading `P` singular vectors.
    /// Ignored if `compute_uv` is `False`.
    ///
    /// Defaults to false
    TF_MUST_USE_RESULT Attrs FullMatrices(bool x) {
      Attrs ret = *this;
      ret.full_matrices_ = x;
      return ret;
    }

    bool compute_uv_ = true;
    bool full_matrices_ = false;
  };
  Svd(const ::tensorflow::Scope& scope, ::tensorflow::Input input);
  Svd(const ::tensorflow::Scope& scope, ::tensorflow::Input input, const
    Svd::Attrs& attrs);

  static Attrs ComputeUv(bool x) {
    return Attrs().ComputeUv(x);
  }
  static Attrs FullMatrices(bool x) {
    return Attrs().FullMatrices(x);
  }

  ::tensorflow::Output s;
  ::tensorflow::Output u;
  ::tensorflow::Output v;
};

/// @}

}  // namespace ops
}  // namespace tensorflow

#endif  // TENSORFLOW_CC_OPS_LINALG_OPS_H_
