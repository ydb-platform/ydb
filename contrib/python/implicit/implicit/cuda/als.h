#ifndef IMPLICIT_CUDA_ALS_H_
#define IMPLICIT_CUDA_ALS_H_
#include "implicit/cuda/matrix.h"

// Forward ref: don't require the whole cublas definition here
struct cublasContext;

namespace implicit {

struct CudaLeastSquaresSolver {
    explicit CudaLeastSquaresSolver(int factors);
    ~CudaLeastSquaresSolver();

    void least_squares(const CudaCSRMatrix & Cui,
                       CudaDenseMatrix * X, const CudaDenseMatrix & Y,
                       float regularization,
                       int cg_steps) const;

    float calculate_loss(const CudaCSRMatrix & Cui,
                        const CudaDenseMatrix & X,
                        const CudaDenseMatrix & Y,
                        float regularization);

    CudaDenseMatrix YtY;
    cublasContext * blas_handle;
};
}  // namespace implicit
#endif  // IMPLICIT_CUDA_ALS_H_
