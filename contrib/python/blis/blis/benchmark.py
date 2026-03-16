# Copyright ExplsionAI GmbH, released under BSD.
import numpy
import numpy.random
from .py import gemm, einsum
from timeit import default_timer as timer

numpy.random.seed(0)


def create_data(nO, nI, batch_size):
    X = numpy.zeros((batch_size, nI), dtype="f")
    X += numpy.random.uniform(-1.0, 1.0, X.shape)
    W = numpy.zeros((nO, nI), dtype="f")
    W += numpy.random.uniform(-1.0, 1.0, W.shape)
    return X, W


def get_numpy_blas():
    blas_libs = numpy.__config__.blas_opt_info["libraries"]
    return blas_libs[0]


def numpy_gemm(X, W, n=1000):
    nO, nI = W.shape
    batch_size = X.shape[0]
    total = 0.0
    y = numpy.zeros((batch_size, nO), dtype="f")
    for i in range(n):
        numpy.dot(X, W, out=y)
        total += y.sum()
        y.fill(0)
    print("Total:", total)


def blis_gemm(X, W, n=1000):
    nO, nI = W.shape
    batch_size = X.shape[0]
    total = 0.0
    y = numpy.zeros((batch_size, nO), dtype="f")
    for i in range(n):
        gemm(X, W, out=y)
        total += y.sum()
        y.fill(0.0)
    print("Total:", total)


def numpy_einsum(X, W, n=1000):
    nO, nI = W.shape
    batch_size = X.shape[0]
    total = 0.0
    y = numpy.zeros((nO, batch_size), dtype="f")
    for i in range(n):
        numpy.einsum("ab,cb->ca", X, W, out=y)
        total += y.sum()
        y.fill(0.0)
    print("Total:", total)


def blis_einsum(X, W, n=1000):
    nO, nI = W.shape
    batch_size = X.shape[0]
    total = 0.0
    y = numpy.zeros((nO, batch_size), dtype="f")
    for i in range(n):
        einsum("ab,cb->ca", X, W, out=y)
        total += y.sum()
        y.fill(0.0)
    print("Total:", total)


def main(nI=128 * 3, nO=128 * 3, batch_size=2000):
    print(
        "Setting up data for gemm. 1000 iters,  "
        "nO={nO} nI={nI} batch_size={batch_size}".format(**locals())
    )
    numpy_blas = get_numpy_blas()
    X1, W1 = create_data(nI, nO, batch_size)
    X2 = X1.copy()
    W2 = W1.copy()
    print("Blis gemm...")
    start = timer()
    blis_gemm(X2, W2, n=1000)
    end = timer()
    blis_time = end - start
    print("%.2f seconds" % blis_time)
    print("Numpy (%s) gemm..." % numpy_blas)
    start = timer()
    numpy_gemm(X1, W1)
    end = timer()
    numpy_time = end - start
    print("%.2f seconds" % numpy_time)
    print("Blis einsum ab,cb->ca")
    start = timer()
    blis_einsum(X2, W2, n=1000)
    end = timer()
    blis_time = end - start
    print("%.2f seconds" % blis_time)
    print("Numpy (%s) einsum ab,cb->ca" % numpy_blas)
    start = timer()
    numpy_einsum(X2, W2)
    end = timer()
    numpy_time = end - start
    print("%.2f seconds" % numpy_time)


if __name__ == '__main__':
    main()
