#ifndef IMPLICIT_CUDA_MATRIX_H_
#define IMPLICIT_CUDA_MATRIX_H_

namespace implicit {
/// Thin wrappers of CUDA memory: copies to from host, frees in destructor
template <typename T>
struct CudaVector {
    CudaVector(int size, const T * elements);
    ~CudaVector();

    int size;
    T * data;
};

struct CudaCSRMatrix {
    CudaCSRMatrix(int rows, int cols, int nonzeros,
                  const int * indptr, const int * indices, const float * data);
    ~CudaCSRMatrix();
    int * indptr, * indices;
    float * data;
    int rows, cols, nonzeros;
};

struct CudaCOOMatrix {
    CudaCOOMatrix(int rows, int cols, int nonzeros,
                  const int * row, const int * col, const float * data);
    ~CudaCOOMatrix();
    int * row, * col;
    float * data;
    int rows, cols, nonzeros;
};

struct CudaDenseMatrix {
    CudaDenseMatrix(int rows, int cols, const float * data);
    ~CudaDenseMatrix();

    void to_host(float * output) const;

    int rows, cols;
    float * data;
};
}  // namespace implicit
#endif  // IMPLICIT_CUDA_MATRIX_H_
