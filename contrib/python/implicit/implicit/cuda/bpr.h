#ifndef IMPLICIT_CUDA_BPR_H_
#define IMPLICIT_CUDA_BPR_H_
#include "implicit/cuda/matrix.h"
#include <utility>

namespace implicit {
std::pair<int, int>  bpr_update(const CudaVector<int> & userids,
                                const CudaVector<int> & itemids,
                                const CudaVector<int> & indptr,
                                CudaDenseMatrix * X,
                                CudaDenseMatrix * Y,
                                float learning_rate,
                                float regularization,
                                long seed,
                                bool verify_negative_samples);
}  // namespace implicit
#endif  // IMPLICIT_CUDA_BPR_H_
