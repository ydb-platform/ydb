#include <math.h>
#include <stdio.h>

#include <cuda_runtime.h>
#include "cublas_v2.h"

#include "implicit/cuda/als.h"
#include "implicit/cuda/utils.cuh"

namespace implicit {

using std::invalid_argument;

__global__ void least_squares_cg_kernel(int factors, int user_count, int item_count,
                                        float * X, const float * Y, const float * YtY,
                                        const int * indptr, const int * indices,
                                        const float * data, int cg_steps) {
    // Ap/r/p are vectors for CG update - use dynamic shared memory to store
    // https://devblogs.nvidia.com/parallelforall/using-shared-memory-cuda-cc/
    extern __shared__ float shared_memory[];
    float * Ap = &shared_memory[0];
    float * r = &shared_memory[factors];
    float * p = &shared_memory[2*factors];

    // Stride over users in the grid:
    // https://devblogs.nvidia.com/parallelforall/cuda-pro-tip-write-flexible-kernels-grid-stride-loops/
    for (int u = blockIdx.x; u < user_count; u += gridDim.x) {
        float * x = &X[u * factors];

        // handle 0-sized rows
        if (indptr[u] == indptr[u+1]) {
            x[threadIdx.x] = 0;
            continue;
        }

        // calculate residual r = YtCuPu - YtCuY Xu
        float temp = 0;
        for (int i = 0; i < factors; ++i) {
            temp -= x[i] * YtY[i * factors + threadIdx.x];
        }
        for (int index = indptr[u]; index < indptr[u + 1]; ++index) {
            const float * Yi = &Y[indices[index] * factors];
            float confidence = data[index];

            if (confidence > 0) {
                temp += (confidence - (confidence - 1) * dot(Yi, x)) * Yi[threadIdx.x];
            } else {
                confidence *= -1;
                temp += (- (confidence - 1) * dot(Yi, x)) * Yi[threadIdx.x];
            }
        }
        p[threadIdx.x] = r[threadIdx.x] = temp;

        float rsold = dot(r, r);
        if (rsold < 1e-20) continue;

        for (int it = 0; it < cg_steps; ++it) {
            // calculate Ap = YtCuYp - without actually calculating YtCuY
            Ap[threadIdx.x] = 0;
            for (int i = 0; i < factors; ++i) {
                Ap[threadIdx.x] += p[i] * YtY[i * factors + threadIdx.x];
            }
            for (int index = indptr[u]; index < indptr[u + 1]; ++index) {
                const float * Yi = &Y[indices[index] * factors];
                float confidence = data[index];
                if (confidence < 0) confidence *= -1;

                Ap[threadIdx.x] += (confidence - 1) * dot(Yi, p) * Yi[threadIdx.x];
            }

            // standard CG update
            float alpha = rsold / dot(p, Ap);
            x[threadIdx.x] += alpha * p[threadIdx.x];
            r[threadIdx.x] -= alpha * Ap[threadIdx.x];
            float rsnew = dot(r, r);
            if (rsnew < 1e-20) break;
            p[threadIdx.x] = r[threadIdx.x] + (rsnew/rsold) * p[threadIdx.x];
            rsold = rsnew;
            __syncthreads();
        }

        // this shouldn't happen - but if we hit a NaN in the above code then complain
        // and don't let it perpetuate
        if (isnan(rsold)) {
            if (threadIdx.x == 0) {
                printf("Warning NaN Detected in row %d of %d\n", u, user_count);
            }
            x[threadIdx.x] = 0;
        }
    }
}

__global__ void l2_regularize_kernel(int factors, float regularization, float * YtY) {
    YtY[threadIdx.x * factors + threadIdx.x] += regularization;
}
CudaLeastSquaresSolver::CudaLeastSquaresSolver(int factors)
    : YtY(factors, factors, NULL) {
    CHECK_CUBLAS(cublasCreate(&blas_handle));
}

void CudaLeastSquaresSolver::least_squares(const CudaCSRMatrix & Cui,
                                           CudaDenseMatrix * X,
                                           const CudaDenseMatrix & Y,
                                           float regularization,
                                           int cg_steps) const {
    int item_count = Y.rows, user_count = X->rows, factors = X->cols;
    if (X->cols != Y.cols) throw invalid_argument("X and Y should have the same number of columns");
    if (X->cols != YtY.cols) throw invalid_argument("Columns of X don't match number of factors");
    if (Cui.rows != X->rows) throw invalid_argument("Dimensionality mismatch between Cui and X");
    if (Cui.cols != Y.rows) throw invalid_argument("Dimensionality mismatch between Cui and Y");

    // calculate YtY: note this expects col-major (and we have row-major basically)
    // so that we're inverting the CUBLAS_OP_T/CU_BLAS_OP_N ordering to overcome
    // this (like calculate YYt instead of YtY)
    float alpha = 1.0, beta = 0.;
    CHECK_CUBLAS(cublasSgemm(blas_handle, CUBLAS_OP_N, CUBLAS_OP_T,
                             factors, factors, item_count,
                             &alpha,
                             Y.data, factors,
                             Y.data, factors,
                             &beta,
                             YtY.data, factors));
    CHECK_CUDA(cudaDeviceSynchronize());

    // regularize the matrix
    l2_regularize_kernel<<<1, factors>>>(factors, regularization, YtY.data);
    CHECK_CUDA(cudaDeviceSynchronize());

    // TODO: multi-gpu support
    int devId;
    CHECK_CUDA(cudaGetDevice(&devId));

    int multiprocessor_count;
    CHECK_CUDA(cudaDeviceGetAttribute(&multiprocessor_count,
                                      cudaDevAttrMultiProcessorCount,
                                      devId));

    int block_count = 128 * multiprocessor_count;
    int thread_count = factors;
    int shared_memory_size = sizeof(float) * (3 * factors);

    least_squares_cg_kernel<<<block_count, thread_count, shared_memory_size>>>(
        factors, user_count, item_count,
        X->data, Y.data, YtY.data, Cui.indptr, Cui.indices, Cui.data, cg_steps);

    CHECK_CUDA(cudaDeviceSynchronize());
}

__global__
void calculate_loss_kernel(int factors, int user_count, int item_count,
                           const float * X, const float * Y, const float * YtY,
                           const int * indptr, const int * indices,
                           const float * data, float regularization, float * output) {
    // https://devblogs.nvidia.com/parallelforall/using-shared-memory-cuda-cc/
    extern __shared__ float shared_memory[];
    float * r = &shared_memory[0];

    float loss = 0, user_norm = 0, item_norm = 0, total_confidence = 0;

    for (int u = blockIdx.x; u < user_count; u += gridDim.x) {
        const float * x = &X[u * factors];

        // calculates r = (YtCuY.dot(Xu) - 2 * YtCuPu).dot(Xu), without calculating YtCuY
        float temp = 0;
        for (int i = 0; i < factors; ++i) {
            temp += x[i] * YtY[i * factors + threadIdx.x];
        }

        for (int index = indptr[u]; index < indptr[u + 1]; ++index) {
            const float * Yi = &Y[indices[index] * factors];
            float confidence = data[index];
            if (confidence > 0) {
                temp += ((confidence - 1 ) * dot(Yi, x) - 2 * confidence) * Yi[threadIdx.x];
            } else {
                confidence *= -1;
                temp += ((confidence - 1 ) * dot(Yi, x)) * Yi[threadIdx.x];
            }
            loss += confidence;
            total_confidence += confidence;
        }
        r[threadIdx.x] = temp;
        loss += dot(x, r);

        user_norm += dot(x, x);
    }
    for (int i = blockIdx.x; i < item_count; i += gridDim.x) {
        const float * y = &Y[i * factors];
        item_norm += dot(y, y);
    }

    loss += regularization * (item_norm + user_norm);
    if (threadIdx.x == 0) {
        atomicAdd(output, loss);
        atomicAdd(output + 1, total_confidence);
    }
}

float CudaLeastSquaresSolver::calculate_loss(const CudaCSRMatrix & Cui,
                                            const CudaDenseMatrix & X,
                                            const CudaDenseMatrix & Y,
                                            float regularization) {
    int item_count = Y.rows, factors = Y.cols, user_count = X.rows;

    float alpha = 1.0, beta = 0.;
    CHECK_CUBLAS(cublasSgemm(blas_handle, CUBLAS_OP_N, CUBLAS_OP_T,
                             factors, factors, item_count,
                             &alpha,
                             Y.data, factors,
                             Y.data, factors,
                             &beta,
                             YtY.data, factors));
    CHECK_CUDA(cudaDeviceSynchronize());
    float temp[2] = {0, 0};
    CudaDenseMatrix output(2, 1, temp);
    calculate_loss_kernel<<<1024, factors, sizeof(float) * factors>>>(
        factors, user_count, item_count, X.data, Y.data, YtY.data,
        Cui.indptr, Cui.indices, Cui.data, regularization, output.data);
    CHECK_CUDA(cudaDeviceSynchronize());
    output.to_host(temp);

    return temp[0] / (temp[1] + Cui.rows * Cui.cols - Cui.nonzeros);
}

CudaLeastSquaresSolver::~CudaLeastSquaresSolver() {
    CHECK_CUBLAS(cublasDestroy(blas_handle));
}
}  // namespace implicit
