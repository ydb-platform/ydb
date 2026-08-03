/**
 * Non-metric Space Library
 *
 * Main developers: Bilegsaikhan Naidan, Leonid Boytsov, Yury Malkov, Ben Frederickson, David Novak
 *
 * For the complete list of contributors and further details see:
 * https://github.com/nmslib/nmslib
 *
 * Copyright (c) 2013-2018
 *
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 */
#ifdef _MSC_VER
#define _SCL_SECURE_NO_WARNINGS
#endif

#include <iostream>
#include <memory>
#include <cmath>

#include "init.h"
#include "space.h"
#include "space/space_leven.h"
#include "space/space_sparse_lp.h"
#include "space/space_sparse_scalar.h"
#include "space/space_sparse_vector_inter.h"
#include "space/space_sparse_scalar_fast.h"
#include "space/space_sparse_vector.h"
#include "space/space_scalar.h"
#include "space/space_sparse_jaccard.h"
#ifdef WITH_EXTRAS
#include "space/space_sqfd.h"
#endif
#include "distcomp.h"
#include "permutation_utils.h"
#include "ztimer.h"
#include "pow.h"

#include "../test/testdataset.h"

#define TEST_SPEED_ LP 1
#define RANGE          8.0f
#define RANGE_SMALL    1e-6f

namespace similarity {

using std::unique_ptr;
using std::vector;


template <class T> 
inline void Normalize(T* pVect, size_t qty) {
  T sum = 0;
  for (size_t i = 0; i < qty; ++i) {
    sum += pVect[i];
  }
  if (sum != 0) {
    for (size_t i = 0; i < qty; ++i) {
      pVect[i] /= sum;
    }
  }
}


template <class T> 
inline void GenRandVect(T* pVect, size_t qty, T MinElem = T(0), T MaxElem = T(1), bool DoNormalize = false) {
  T sum = 0;
  for (size_t i = 0; i < qty; ++i) {
    pVect[i] = MinElem + (MaxElem - MinElem) * RandomReal<T>();
    sum += fabs(pVect[i]);
  }
  if (DoNormalize && sum != 0) {
    for (size_t i = 0; i < qty; ++i) {
      pVect[i] /= sum;
    }
  }
}

inline void GenRandIntVect(int* pVect, size_t qty) {
  for (size_t i = 0; i < qty; ++i) {
    pVect[i] = RandomInt();
  }
}

template <class T> 
inline void SetRandZeros(T* pVect, size_t qty, double pZero) {
    for (size_t j = 0; j < qty; ++j) if (RandomReal<T>() < pZero) pVect[j] = T(0);
}

using namespace std;


/*

#ifdef  __INTEL_COMPILER

// TODO: @Leo figure out how to use this function with Intel and cmake

#include "mkl.h"

TEST(set_intel) {
    vmlSetMode(VML_HA);
    LOG(LIB_INFO) << "Set high-accuracy mode.";
}

#endif

*/


template <typename dist_t>
void GenSparseVectZipf(size_t maxSize, vector<SparseVectElem<dist_t>>& res) {
  maxSize = max(maxSize, (size_t)1);

  for (size_t i = 1; i < maxSize; ++i) {
    float f = RandomReal<float>();
    if (f <= sqrt((float)i)/i) { // This is a bit ad hoc, but is ok for testing purposes
      res.push_back(SparseVectElem<dist_t>(i, RandomReal<float>()));
    }
  }
};

// Efficiency test functions

template <class T>
void TestLInfNormStandard(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;
    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * LInfNormStandard(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard LInfs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestLInfNorm(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;
    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * LInfNorm(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of optim. LInfs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestLInfNormSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];
    T* p = pArr;

    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;
    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * LInfNormSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SIMD LInfs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}


template <class T>
void TestL1NormStandard(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;
    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * L1NormStandard(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard L1s per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestL1Norm(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;
    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * L1Norm(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of optim. L1s per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestL1NormSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];
    T* p = pArr;

    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * L1NormSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SIMD L1s per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestL2NormStandard(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * L2NormStandard(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard L2s per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestL2Norm(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * L2Norm(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of optim. L2s per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestL2NormSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
 
    T fract = T(1)/N;
    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * L2NormSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }
 
    uint64_t tDiff = t.split();
 
    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SIMD L2s per second: " << (1e6/tDiff) * N * Rep ;
 
    delete [] pArr;
 
}


template <class T>
void TestLPGeneric(size_t N, size_t dim, size_t Rep, T power) {
    T* pArr = new T[N * dim];
    T* p = pArr;

    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(-RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * LPGenericDistance(pArr + j*dim, pArr + (j-1)*dim, dim, power) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of Generic L" << power << " per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestLPGenericOptim(size_t N, size_t dim, size_t Rep, T power) {
    T* pArr = new T[N * dim];
    T* p = pArr;

    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(-RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * LPGenericDistanceOptim(pArr + j*dim, pArr + (j-1)*dim, dim, power) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of Optimized generic L" << power << " per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestItakuraSaitoPrecomp(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0));
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * ItakuraSaitoPrecomp(pArr + j*dim*2, pArr + (j-1)*dim*2, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of precomp. ItakuraSaito per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestItakuraSaitoPrecompSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0));
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * ItakuraSaitoPrecompSIMD(pArr + j*dim*2, pArr + (j-1)*dim*2, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SIMD precomp. ItakuraSaito per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestItakuraSaitoStandard(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * ItakuraSaito(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of ItakuraSaito per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}


template <class T>
void TestKLPrecomp(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0), true /* norm. for regular KL */);
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * KLPrecomp(pArr + j*dim*2, pArr + (j-1)*dim*2, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of precomp. KLs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestKLPrecompSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0), true /* norm. for regular KL */);
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * KLPrecompSIMD(pArr + j*dim*2, pArr + (j-1)*dim*2, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SIMD precomp. KLs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestKLStandard(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0), true /* norm. for regular KL */);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * KLStandard(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of KLs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}


template <class T>
void TestKLGeneralPrecomp(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0));
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * KLGeneralPrecomp(pArr + j*dim*2, pArr + (j-1)*dim*2, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of precomp. general. KLs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestKLGeneralPrecompSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0));
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * KLGeneralPrecompSIMD(pArr + j*dim*2, pArr + (j-1)*dim*2, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SIMD precomp. general. KLs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestKLGeneralStandard(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * KLGeneralStandard(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of general. KLs per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestJSStandard(size_t N, size_t dim, size_t Rep, float pZero) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(0), T(1), true);
        SetRandZeros(p, dim, pZero);
        Normalize(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * JSStandard(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of JSs (sparsity:" << pZero << ") per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestJSPrecomp(size_t N, size_t dim, size_t Rep, float pZero) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(0), T(1), true);
        SetRandZeros(p, dim, pZero);
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * JSPrecomp(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of JSs (precomp) (sparsity:" << pZero << ")  per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestJSPrecompApproxLog(size_t N, size_t dim, size_t Rep, float pZero) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(0), T(1), true);
        SetRandZeros(p, dim, pZero);
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * JSPrecompApproxLog(pArr + 2*j*dim, pArr + 2*(j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of JSs (precomp, one log approx) (sparsity:" << pZero << ") per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestJSPrecompSIMDApproxLog(size_t N, size_t dim, size_t Rep, float pZero) {
    T* pArr = new T[N * dim * 2];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= 2 * dim) {
        GenRandVect(p, dim, T(0), T(1), true);
        SetRandZeros(p, dim, pZero);
        PrecompLogarithms(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * JSPrecompSIMDApproxLog(pArr + 2*j*dim, pArr + 2*(j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of JSs (precomp, one log approx, SIMD) (sparsity:" << pZero << ") per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

void TestSpearmanRho(size_t N, size_t dim, size_t Rep) {
    int* pArr = new int[N * dim];

    int *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandIntVect(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0.0f;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * SpearmanRho(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard SpearmanRho per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

void TestSpearmanRhoSIMD(size_t N, size_t dim, size_t Rep) {
    int* pArr = new int[N * dim];

    int *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandIntVect(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0.0f;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * SpearmanRhoSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SpearmanRhoSIMD per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

void TestSpearmanFootrule(size_t N, size_t dim, size_t Rep) {
    int* pArr = new int[N * dim];

    int *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandIntVect(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * SpearmanFootrule(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard SpearmanFootrule per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

void TestSpearmanFootruleSIMD(size_t N, size_t dim, size_t Rep) {
    int* pArr = new int[N * dim];

    int *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandIntVect(p, dim);
    }

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * SpearmanFootruleSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << "Elapsed: " << tDiff / 1e3 << " ms " << " # of SpearmanFootruleSIMD per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

#if defined(WITH_EXTRAS)

template <class T>
void TestSQFDGeneric(size_t N, size_t Rep, SqfdFunction<T>& func) {
    // Space will try to delete the function
    unique_ptr<SpaceSqfd<T>>  space(new SpaceSqfd<T>(func.Clone()));
    ObjectVector              elems;
    vector<string>            tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, (sampleDataPrefix + "sqfd20_10k_10k.txt"), N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) <<  typeid(T).name() << " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of " << space->StrDesc() << " distances per second: " << (1e6/tDiff) * N * Rep ;

}


template <class T>
void TestSQFDMinus(size_t N, size_t Rep) {
  SqfdMinusFunction<T> func;
  TestSQFDGeneric(N, Rep, func);
}

template <class T>
void TestSQFDHeuristic(size_t N, size_t Rep) {
  SqfdHeuristicFunction<T> func(T(1));
  TestSQFDGeneric(N, Rep, func);
}

template <class T>
void TestSQFDGaussian(size_t N, size_t Rep) {
  SqfdGaussianFunction<T> func(T(1));
  TestSQFDGeneric(N, Rep, func);
}

#endif

void TestLevenshtein(size_t N, size_t Rep) {
    unique_ptr<SpaceLevenshtein>  space(new SpaceLevenshtein);
    ObjectVector                  elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, (sampleDataPrefix + "dna32_4_5K.txt"), N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of unoptimized unweighted Levenshtein distances per second: " << (1e6/tDiff) * N * Rep ;

}

template <class T>
void TestSparseLp(size_t N, size_t Rep, T power) {
    unique_ptr<SpaceSparseLp<T>>  space(new SpaceSparseLp<T>(power));
    ObjectVector                  elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, (sampleDataPrefix + "sparse_5K.txt"), N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of sparse LP (p=" << power << ") per second: " << (1e6/tDiff) * N * Rep ;

}

template <class T>
void TestSparseAngularDistance(const string& dataFile, size_t N, size_t Rep) {
    unique_ptr<SpaceSparseAngularDistance<T>>  space(new SpaceSparseAngularDistance<T>());
    ObjectVector                  elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile, N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile 
         << " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of sparse angular dist per second: " << (1e6/tDiff) * N * Rep ;

}

void TestSparseCosineSimilarityFast(const string& dataFile, size_t N, size_t Rep) {
    typedef float T;

    unique_ptr<SpaceSparseCosineSimilarityFast>  space(new SpaceSparseCosineSimilarityFast());
    ObjectVector                                 elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile,  N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile << 
            " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of (fast) sparse cosine similarity dist second: " << (1e6/tDiff) * N * Rep ;

}

void TestSparseAngularDistanceFast(const string& dataFile, size_t N, size_t Rep) {
    typedef float T;

    unique_ptr<SpaceSparseAngularDistanceFast>  space(new SpaceSparseAngularDistanceFast());
    ObjectVector                                 elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile, N));
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1) / N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j - 1], elems[j]) / N;
        }
        /*
        * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
        *
        * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
        */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile <<
        " Elapsed: " << tDiff / 1e3 << " ms " <<
        " # of (fast) sparse angular dist second: " << (1e6 / tDiff) * N * Rep;

}

void TestSparseNegativeScalarProductFast(const string& dataFile, size_t N, size_t Rep) {
    typedef float T;

    unique_ptr<SpaceSparseNegativeScalarProductFast>  space(new SpaceSparseNegativeScalarProductFast());
    ObjectVector                                      elems;
    vector<string>                                    tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile,  N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile << 
            " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of (fast) negative scalar/dot product dist second: " << (1e6/tDiff) * N * Rep ;

}

void TestSparseQueryNormNegativeScalarProductFast(const string& dataFile, size_t N, size_t Rep) {
    typedef float T;

    unique_ptr<SpaceSparseQueryNormNegativeScalarProductFast>  space(new SpaceSparseQueryNormNegativeScalarProductFast());
    ObjectVector                                      elems;
    vector<string>                                    tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile,  N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile << 
            " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of (fast) QUERY-NORMALIZED negative scalar/dot product dist second: " << (1e6/tDiff) * N * Rep ;

}


template <class T>
void TestSparseCosineSimilarity(const string& dataFile, size_t N, size_t Rep) {
    unique_ptr<SpaceSparseCosineSimilarity<T>>  space(new SpaceSparseCosineSimilarity<T>());
    ObjectVector                  elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile, N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile 
         << " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of sparse cosine similarity dist second: " << (1e6/tDiff) * N * Rep ;

}

template <class T>
void TestSparseNegativeScalarProduct(const string& dataFile, size_t N, size_t Rep) {
    unique_ptr<SpaceSparseNegativeScalarProduct<T>>  space(new SpaceSparseNegativeScalarProduct<T>());
    ObjectVector                  elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile, N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile 
         << " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of negative scalar product dist second: " << (1e6/tDiff) * N * Rep ;

}

template <class T>
void TestSparseQueryNormNegativeScalarProduct(const string& dataFile, size_t N, size_t Rep) {
    unique_ptr<SpaceSparseQueryNormNegativeScalarProduct<T>>  space(new SpaceSparseQueryNormNegativeScalarProduct<T>());
    ObjectVector                  elems;
    vector<string>                tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile, N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile 
         << " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of QUERY-NORMALIZED negative scalar product dist second: " << (1e6/tDiff) * N * Rep ;

}
template <class T>
void TestScalarProduct(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * ScalarProduct(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of ScalarProduct per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestScalarProductSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * ScalarProductSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of ScalarProduct SIMD per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestNormScalarProduct(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * NormScalarProduct(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of NormScalarProduct per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestNormScalarProductSIMD(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * NormScalarProductSIMD(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of NormScalarProduct SIMD per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestCosineSimilarity(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * CosineSimilarity(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard CosineSimilarity per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestAngularDistance(size_t N, size_t dim, size_t Rep) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, -T(RANGE), T(RANGE));
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;
    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * AngularDistance(pArr + j*dim, pArr + (j-1)*dim, dim) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of standard AngularDistance per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

void TestBitHamming(size_t N, size_t dim, size_t Rep) {
    size_t WordQty = (dim + 31)/32; 
    uint32_t* pArr = new uint32_t[N * WordQty];

    uint32_t *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= WordQty) {
        vector<PivotIdType> perm(dim);
        GenRandIntVect(&perm[0], dim);
        for (unsigned j = 0; j < dim; ++j)
          perm[j] = perm[j] % 2;
        vector<uint32_t> h;
        Binarize(perm, 1, h); 
        CHECK(h.size() == WordQty);
        memcpy(p, &h[0], WordQty * sizeof(h[0]));
    }

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * BitHamming(pArr + j*WordQty, pArr + (j-1)*WordQty, WordQty) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << "Elapsed: " << tDiff / 1e3 << " ms " << " # of BitHamming per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

void TestBitJaccard(size_t N, size_t dim, size_t Rep) {
    size_t WordQty = (dim + 31)/32; 
    uint32_t* pArr = new uint32_t[N * WordQty];

    uint32_t *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= WordQty) {
        vector<PivotIdType> perm(dim);
        GenRandIntVect(&perm[0], dim);
        for (unsigned j = 0; j < dim; ++j)
          perm[j] = perm[j] % 2;
        vector<uint32_t> h;
        Binarize(perm, 1, h); 
        CHECK(h.size() == WordQty);
        memcpy(p, &h[0], WordQty * sizeof(h[0]));
    }

    WallClockTimer  t;

    t.reset();

    float DiffSum = 0;

    float fract = 1.0f/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * BitJaccard<float, uint32_t>(pArr + j*WordQty, pArr + (j-1)*WordQty, WordQty) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << "Elapsed: " << tDiff / 1e3 << " ms " << " # of BitJaccard per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class dist_t>
void TestSparseJaccardSimilarity(const string& dataFile, size_t N, size_t Rep) {
    typedef float T;

    unique_ptr<SpaceSparseJaccard<dist_t>>  space(new SpaceSparseJaccard<dist_t>());
    ObjectVector                            elems;
    vector<string>                          tmp;

    unique_ptr<DataFileInputState> inpState(space->ReadDataset(elems, tmp, dataFile,  N)); 
    space->UpdateParamsFromFile(*inpState);

    N = min(N, elems.size());

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * space->IndexTimeDistance(elems[j-1], elems[j]) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " File: " << dataFile << 
            " Elapsed: " << tDiff / 1e3 << " ms " << 
            " # of sparse Jaccard similarity dist second: " << (1e6/tDiff) * N * Rep ;

}

template <class T>
void TestRenyiDivSlow(size_t N, size_t dim, size_t Rep, T alpha) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0), true /* norm. for regular KL */);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * renyiDivergenceSlow(pArr + j*dim, pArr + (j-1)*dim, dim, alpha) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of slow Renyi-div. (alpha=" << alpha << ") per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

template <class T>
void TestRenyiDivFast(size_t N, size_t dim, size_t Rep, T alpha) {
    T* pArr = new T[N * dim];

    T *p = pArr;
    for (size_t i = 0; i < N; ++i, p+= dim) {
        GenRandVect(p, dim, T(RANGE_SMALL), T(1.0), true /* norm. for regular KL */);
    }

    WallClockTimer  t;

    t.reset();

    T DiffSum = 0;

    T fract = T(1)/N;

    for (size_t i = 0; i < Rep; ++i) {
        for (size_t j = 1; j < N; ++j) {
            DiffSum += 0.01f * renyiDivergenceFast(pArr + j*dim, pArr + (j-1)*dim, dim, alpha) / N;
        }
        /* 
         * Multiplying by 0.01 and dividing the sum by N is to prevent Intel from "cheating":
         *
         * http://searchivarius.org/blog/problem-previous-version-intels-library-benchmark
         */
        DiffSum *= fract;
    }

    uint64_t tDiff = t.split();

    LOG(LIB_INFO) << "Ignore: " << DiffSum;
    LOG(LIB_INFO) << typeid(T).name() << " " << "Elapsed: " << tDiff / 1e3 << " ms " << " # of fast Renyi-div. (alpha=" << alpha << ") per second: " << (1e6/tDiff) * N * Rep ;

    delete [] pArr;

}

}  // namespace similarity

using namespace similarity;

int main(int argc, char* argv[]) {
    string LogFile;
    if (argc == 2) LogFile = argv[1];
    initLibrary(0, LogFile.empty() ? LIB_LOGSTDERR:LIB_LOGFILE, LogFile.c_str());

    int nTest  = 0;

    int dim = 128;

    float alphaStepSlow = 1.0f/4;
    for (float alpha = alphaStepSlow; alpha <= 2; alpha+= alphaStepSlow) {
      if (fabs(alpha - 1) < 1e-6) continue;
      nTest++;
      TestRenyiDivSlow<float>(1024, dim, 100, alpha);
    }

    float alphaStepFast = 1.0f/32;
    for (float alpha = alphaStepFast; alpha <= 2; alpha+= alphaStepFast) {
      if (fabs(alpha - 1) < 1e-6) continue;
      nTest++;
      TestRenyiDivFast<float>(1024, dim, 100, alpha);
    }

#if defined(WITH_EXTRAS)
    nTest++;
    TestSQFDMinus<float>(2000, 50);
    nTest++;
    TestSQFDHeuristic<float>(2000, 50);
    nTest++;
    TestSQFDGaussian<float>(2000, 50);
#endif

    nTest++;
    TestLevenshtein(10000, 50);

    nTest++;
    TestBitHamming(1000, 32, 50000);
    nTest++;
    TestBitHamming(1000, 64, 25000);
    nTest++;
    TestBitHamming(1000, 128, 12000);
    nTest++;
    TestBitHamming(1000, 256, 6000);
    nTest++;
    TestBitHamming(1000, 512, 3000);
    nTest++;
    TestBitHamming(1000, 1024, 1500);

    nTest++;
    TestBitJaccard(1000, 32, 50000);
    nTest++;
    TestBitJaccard(1000, 64, 25000);
    nTest++;
    TestBitJaccard(1000, 128, 12000);
    nTest++;
    TestBitJaccard(1000, 256, 6000);
    nTest++;
    TestBitJaccard(1000, 512, 3000);
    nTest++;
    TestBitJaccard(1000, 1024, 1500);

    float pZero1 = 0.5f;
    float pZero2 = 0.25f;
    float pZero3 = 0.0f;

    nTest++;
    TestScalarProduct<float>(1000, dim, 1000);
    nTest++;
    TestScalarProductSIMD<float>(1000, dim, 1000);
    nTest++;


    nTest++;
    TestNormScalarProduct<float>(1000, dim, 1000);
    nTest++;
    TestNormScalarProductSIMD<float>(1000, dim, 1000);
    nTest++;


    TestCosineSimilarity<float>(1000, dim, 1000);
    nTest++;
    TestAngularDistance<float>(1000, dim, 1000);

    nTest++;
    TestSparseCosineSimilarityFast(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseCosineSimilarityFast(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);
    nTest++;
    TestSparseNegativeScalarProductFast(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseNegativeScalarProductFast(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);
    nTest++;
    TestSparseQueryNormNegativeScalarProductFast(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseQueryNormNegativeScalarProductFast(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);
    nTest++;
    TestSparseAngularDistanceFast(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseAngularDistanceFast(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);

    nTest++;
    TestSparseCosineSimilarity<float>(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseCosineSimilarity<float>(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);
    nTest++;
    TestSparseAngularDistance<float>(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseAngularDistance<float>(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);
    nTest++;
    TestSparseNegativeScalarProduct<float>(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseNegativeScalarProduct<float>(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);
    nTest++;
    TestSparseQueryNormNegativeScalarProduct<float>(sampleDataPrefix + "sparse_5K.txt", 1000, 1000);
    nTest++;
    TestSparseQueryNormNegativeScalarProduct<float>(sampleDataPrefix + "sparse_wiki_5K.txt", 1000, 1000);

    nTest++;
    TestSparseJaccardSimilarity<float>(sampleDataPrefix + "sparse_ids_5K.txt", 1000, 1000);

    LOG(LIB_INFO) << "Single-precision (sparse) LP-distance tests";
    nTest++;
    TestSparseLp<float>(1000, 1000, -1);
    nTest++;
    nTest++;
    TestSparseLp<float>(1000, 1000, 1/3.0f);

    nTest++;
    TestSpearmanRho(1024, dim, 2000);

    nTest++;
    TestSpearmanRhoSIMD(1024, dim, 2000);

    nTest++;
    TestSpearmanFootrule(1024, dim, 2000);

    nTest++;
    TestSpearmanFootruleSIMD(1024, dim, 2000);

    nTest++;
    TestJSStandard<float>(1024, dim, 1000, pZero1);
    nTest++;
    TestJSStandard<float>(1024, dim, 1000, pZero2);
    nTest++;
    TestJSStandard<float>(1024, dim, 1000, pZero3);

    nTest++;
    TestJSPrecomp<float>(1024, dim, 500, pZero1);
    nTest++;
    TestJSPrecomp<float>(1024, dim, 500, pZero2);
    nTest++;
    TestJSPrecomp<float>(1024, dim, 500, pZero3);

    nTest++;
    TestJSPrecompApproxLog<float>(1024, dim, 1000, pZero1);
    nTest++;
    TestJSPrecompApproxLog<float>(1024, dim, 1000, pZero2);
    nTest++;
    TestJSPrecompApproxLog<float>(1024, dim, 1000, pZero3);

    nTest++;
    TestJSPrecompSIMDApproxLog<float>(1024, dim, 2000, pZero1);
    nTest++;
    TestJSPrecompSIMDApproxLog<float>(1024, dim, 2000, pZero2);
    nTest++;
    TestJSPrecompSIMDApproxLog<float>(1024, dim, 2000, pZero3);


    nTest++;
    TestL1Norm<float>(1024, dim, 10000);

    nTest++;
    TestL1NormStandard<float>(1024, dim, 10000);

    nTest++;
    TestL1NormSIMD<float>(1024, dim, 10000);

    nTest++;
    TestLInfNorm<float>(1024, dim, 10000);

    nTest++;
    TestLInfNormStandard<float>(1024, dim, 10000);

    nTest++;
    TestLInfNormSIMD<float>(1024, dim, 10000);

    nTest++;
    TestItakuraSaitoStandard<float>(1024, dim, 1000);


    nTest++;
    TestItakuraSaitoPrecomp<float>(1024, dim, 2000);

    nTest++;
    TestItakuraSaitoPrecompSIMD<float>(1024, dim, 4000);

    nTest++;
    TestL2Norm<float>(1024, dim, 10000);

    nTest++;
    TestL2NormStandard<float>(1024, dim, 10000);

    nTest++;
    TestL2NormSIMD<float>(1024, dim, 10000);


    nTest++;
    TestKLStandard<float>(1024, dim, 1000);


    nTest++;
    TestKLPrecomp<float>(1024, dim, 2000);

    nTest++;
    TestKLPrecompSIMD<float>(1024, dim, 4000);

    nTest++;
    TestKLGeneralStandard<float>(1024, dim, 1000);

    nTest++;
    TestKLGeneralPrecomp<float>(1024, dim, 2000);

    nTest++;
    TestKLGeneralPrecompSIMD<float>(1024, dim, 2000);

#if TEST_SPEED_LP
    float delta = 0.125/2.0;

    for (float power = delta, step = delta; power <= 24; power += step) {
      nTest++;
      // This one should use an optimized LP function
      TestSparseLp<float>(1000, 1000, power);
      if (power == 3) step = 0.125;
      if (power == 8) step = 0.5;
    }
    LOG(LIB_INFO) << "========================================";

    LOG(LIB_INFO) << "========================================";

    LOG(LIB_INFO) << "Single-precision LP-distance tests";
    for (float power = delta, step = delta; power <= 24; power += step) {
      nTest++;
      TestLPGeneric<float>(128, dim, 200, power);
      nTest++;
      TestLPGenericOptim<float>(128, dim, 200, power);
      if (power == 3) step = 0.125;
      if (power == 8) step = 0.5;
    }
    LOG(LIB_INFO) << "========================================";
#endif

    LOG(LIB_INFO) << "Dimensionality of dense vectors: " << dim; 
    LOG(LIB_INFO) << " " << nTest << " tests performed";
    return 0;
}

