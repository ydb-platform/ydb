
#include <cuda_runtime.h>
#include "cublas_v2.h"

#include "implicit/cuda/matrix.h"
#include "implicit/cuda/utils.cuh"

namespace implicit {
template <typename T>
CudaVector<T>::CudaVector(int size, const T * host_data)
    : size(size) {
    CHECK_CUDA(cudaMalloc(&data, size * sizeof(T)));
    if (host_data) {
        CHECK_CUDA(cudaMemcpy(data, host_data, size * sizeof(T), cudaMemcpyHostToDevice));
    }
}


template <typename T>
CudaVector<T>::~CudaVector() {
    CHECK_CUDA(cudaFree(data));
}

template struct CudaVector<int>;
template struct CudaVector<float>;

CudaDenseMatrix::CudaDenseMatrix(int rows, int cols, const float * host_data)
    : rows(rows), cols(cols) {
    CHECK_CUDA(cudaMalloc(&data, rows * cols * sizeof(float)));
    if (host_data) {
        CHECK_CUDA(cudaMemcpy(data, host_data, rows * cols * sizeof(float), cudaMemcpyHostToDevice));
    }
}

void CudaDenseMatrix::to_host(float * out) const {
    CHECK_CUDA(cudaMemcpy(out, data, rows * cols * sizeof(float), cudaMemcpyDeviceToHost));
}

CudaDenseMatrix::~CudaDenseMatrix() {
    CHECK_CUDA(cudaFree(data));
}

CudaCSRMatrix::CudaCSRMatrix(int rows, int cols, int nonzeros,
                             const int * indptr_, const int * indices_, const float * data_)
    : rows(rows), cols(cols), nonzeros(nonzeros) {

    CHECK_CUDA(cudaMalloc(&indptr, (rows + 1) * sizeof(int)));
    CHECK_CUDA(cudaMemcpy(indptr, indptr_, (rows + 1)*sizeof(int), cudaMemcpyHostToDevice));

    CHECK_CUDA(cudaMalloc(&indices, nonzeros * sizeof(int)));
    CHECK_CUDA(cudaMemcpy(indices, indices_, nonzeros * sizeof(int), cudaMemcpyHostToDevice));

    CHECK_CUDA(cudaMalloc(&data, nonzeros * sizeof(float)));
    CHECK_CUDA(cudaMemcpy(data, data_, nonzeros * sizeof(int), cudaMemcpyHostToDevice));
}

CudaCSRMatrix::~CudaCSRMatrix() {
    CHECK_CUDA(cudaFree(indices));
    CHECK_CUDA(cudaFree(indptr));
    CHECK_CUDA(cudaFree(data));
}

CudaCOOMatrix::CudaCOOMatrix(int rows, int cols, int nonzeros,
                             const int * row_, const int * col_, const float * data_)
    : rows(rows), cols(cols), nonzeros(nonzeros) {

    CHECK_CUDA(cudaMalloc(&row, nonzeros * sizeof(int)));
    CHECK_CUDA(cudaMemcpy(row, row_, nonzeros * sizeof(int), cudaMemcpyHostToDevice));

    CHECK_CUDA(cudaMalloc(&col, nonzeros * sizeof(int)));
    CHECK_CUDA(cudaMemcpy(col, col_, nonzeros * sizeof(int), cudaMemcpyHostToDevice));

    CHECK_CUDA(cudaMalloc(&data, nonzeros * sizeof(float)));
    CHECK_CUDA(cudaMemcpy(data, data_, nonzeros * sizeof(int), cudaMemcpyHostToDevice));
}

CudaCOOMatrix::~CudaCOOMatrix() {
    CHECK_CUDA(cudaFree(row));
    CHECK_CUDA(cudaFree(col));
    CHECK_CUDA(cudaFree(data));
}
}  // namespace implicit
