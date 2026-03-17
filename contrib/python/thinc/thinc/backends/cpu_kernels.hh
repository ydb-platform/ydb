#ifndef CPU_KERNELS_HH
#define CPU_KERNELS_HH

#include <algorithm>
#include <cmath>
#include <cstring>
#include <stdexcept>
#include <string>
#include <type_traits>

// Ideally we'd use an alias declaration for a generic definition of
// *axpy. But Cython doesn't support alias declarations yet:
//
// https://github.com/cython/cython/issues/3272
//
// template <typename T>
// using axpy = void (*)(int N, T alpha, const T* X, int incX,
//                       T *Y, int incY);
//
// So, instead we'll do this the pre-C++11 way:

template <typename T>
struct axpy {
    typedef void (*ptr)(int N, T alpha, const T* X, int incX, T *Y, int incY);
};


// All elementwise functions, such as most activations, work in-place.


template <typename T, typename L>
struct argmax_result {
    T max;
    L max_idx;
};

template <typename T, typename L>
argmax_result<T, L> argmax(T const *arr, L len)
{
    static_assert(std::is_floating_point<T>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    argmax_result<T, L> r { arr[0],  0 };

    for (L i = 1; i < len; ++i) {
        if (arr[i] > r.max) {
            r.max = arr[i];
            r.max_idx = i;
        }
    }

    return r;
}

// The next two templates define argmax for a fixed number of elements.

template <typename T, typename L>
argmax_result<T, L> argmax(T a) {
    static_assert(std::is_floating_point<T>::value, "Argument should be floating point");
    argmax_result<T, L> acc { a, 0 };
    return acc;
}

template<typename T, typename L, typename... Args>
argmax_result<T, L> argmax(T a, Args... args) {
    static_assert(std::is_floating_point<T>::value, "Arguments should be floating point");

    auto acc = argmax<T, L>(args...);

    if (acc.max > a) {
        acc.max_idx += 1;
    } else {
        acc.max_idx = 0;
        acc.max = a;
    }

    return acc;
}


template <typename A, typename L>
void vec_add(A* X, const A* Y, A scale, L N)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (L i = 0; i < N; ++i)
        X[i] += scale * Y[i];
}

template <typename A, typename L>
void cpu_maxout(A* best__bo, L* which__bo, const A* cands__bop, L B, L O, L P)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    // For small inputs, we use an unrolled argmax.
    if (P == 2) {
        for (int i = 0; i < B * O; ++i) {
            A const *input = cands__bop + i * P;
            auto r = argmax<A, L>(input[0], input[1]);
            which__bo[i] = r.max_idx;
            best__bo[i] = r.max;
        }
    } else if (P == 3) {
        for (int i = 0; i < B * O; ++i) {
            A const *input = cands__bop + i * P;
            auto r = argmax<A, L>(input[0], input[1], input[2]);
            which__bo[i] = r.max_idx;
            best__bo[i] = r.max;
        }
    } else {
        for (int i = 0; i < B * O; ++i) {
            auto r = argmax<A, L>(cands__bop + i * P, P);
            which__bo[i] = r.max_idx;
            best__bo[i] = r.max;
        }
    }
}


template <typename A, typename L>
void cpu_backprop_maxout(A* dX__bop, const A* dX__bo, const L* which__bo,
    L B, L O, L P)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (L b = 0; b < B; ++b) {
        for (L o = 0; o < O; ++o) {
            if (*which__bo >= P) {
                throw std::out_of_range(std::string("index ") + std::to_string(*which__bo) + " is out of bounds for maxout with size " + std::to_string(P));
            }

            dX__bop[*which__bo] = *dX__bo;
            dX__bop += P;
            dX__bo += 1;
            which__bo += 1;
        }
    }
}

template <typename A, typename L>
void cpu_reduce_max(A* maxes__bo, L* which__bo, const A* X__to,
    const L* lengths__b, L B, L T, L O)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (const L* length = lengths__b; length < lengths__b + B; ++length) {
        if (*length <= 0)
            throw std::invalid_argument(std::string("all sequence lengths must be > 0, was: ") + std::to_string(*length));
        else if (*length > T) {
            throw std::out_of_range("lengths must sum up to the number of rows");
        }

        T -= *length;

        std::memcpy(maxes__bo, X__to, O * sizeof(*maxes__bo));
        X__to += O;
        for (L i = 1; i < *length; ++i) {
            for (L j = 0; j < O; ++j) {
                if (X__to[j] > maxes__bo[j]) {
                    maxes__bo[j] = X__to[j];
                    which__bo[j] = i;
                }
            }
            X__to += O;
        }

        maxes__bo += O;
        which__bo += O;
    }
}

template <typename A, typename L>
void cpu_backprop_reduce_max(A* dX__to, const A* d_maxes__bo, const L* which__bo,
    const L* lengths__b, L B, L T, L O)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (const L* length = lengths__b; length < lengths__b + B; ++length) {
        for (L i = 0; i < O; ++i) {
            L item = which__bo[i];
            if (item >= *length) {
                throw std::out_of_range(std::string("index ") + std::to_string(item) + " is out of bounds for maxout with length " + std::to_string(*length));
            }

            dX__to[item * O + i] = d_maxes__bo[i];
        }

        dX__to += *length * O;
        d_maxes__bo += O;
        which__bo += O;
    }
}

template <typename A, typename L>
void cpu_reduce_mean(A* means__bo, const A* X__to, const L* lengths__b,
    L B, L T, L O)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (const L* length = lengths__b; length < lengths__b + B; ++length) {
        if (*length < 0) {
            throw std::invalid_argument(std::string("all sequence lengths must be >= 0, was: ") + std::to_string(*length));
        }
        else if (length == 0) {
            means__bo += O;
            continue;
        }
        else if (*length > T) {
            throw std::out_of_range("lengths must sum up to the number of rows");
        }

        T -= *length;

        A scale = 1. / *length;
        for (L i = 0; i < *length; ++i) {
            vec_add(means__bo, X__to, scale, O);
            X__to += O;
        }

        means__bo += O;
    }
}

template <typename A, typename L>
void cpu_backprop_reduce_mean(A* dX__to, const A* d_means__bo, const L* lengths__b,
    L B, L T, L O)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (const L* length = lengths__b; length < lengths__b + B; ++length) {
        A scale = 1. / *length;
        for (L i = 0; i < *length; ++i) {
            vec_add(dX__to, d_means__bo, scale, O);
            dX__to += O;
        }

        d_means__bo += O;
    }
}

template <typename A, typename L>
void cpu_mish(A* Y, L N, A threshold)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (L i = 0; i < N; ++i) {
        if (Y[i] < threshold) {
            Y[i] *= std::tanh(std::log(1.0 + std::exp(Y[i])));
        }
    }
}

template <typename A, typename L>
void cpu_backprop_mish(A* dX, const A* X, L N, A threshold)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (L i = 0; i < N; ++i) {
        A x = X[i];

        if (x < threshold) {
            A exp_x = std::exp(x);
            A exp_2x = std::exp(2 * x);
            A exp_3x = std::exp(3 * x);
            A omega = (4. * (x + 1)) + (4 * exp_2x) + exp_3x + exp_x * (4. * x + 6);
            A delta = 2. * exp_x + exp_2x + 2.;
            dX[i] = dX[i] * ((exp_x * omega) / (delta * delta));
        }
    }
}

template <typename A, typename L>
void cpu_reduce_sum(A* sums__bo, const A* X__to, const L* lengths__b,
    L B, L T, L O)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (const L* length = lengths__b; length < lengths__b + B; ++length) {
        if (*length < 0) {
            throw std::invalid_argument(std::string("all sequence lengths must be >= 0, was: ") + std::to_string(*length));
        }
        else if (length == 0) {
            sums__bo += O;
            continue;
        }
        else if (*length > T) {
            throw std::out_of_range("lengths must sum up to the number of rows");
        }

        T -= *length;

        for (L i = 0; i < *length; ++i) {
            vec_add(sums__bo, X__to, static_cast<A>(1.0), O);
            X__to += O;
        }

        sums__bo += O;
    }
}

template <typename A, typename L>
void cpu_backprop_reduce_sum(A* dX__to, const A* d_sums__bo, const L* lengths__b,
    L B, L T, L O)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (const L* length = lengths__b; length < lengths__b + B; ++length) {
        for (L i = 0; i < *length; ++i) {
            vec_add(dX__to, d_sums__bo, static_cast<A>(1.0), O);
            dX__to += O;
        }

        d_sums__bo += O;
    }
}

template <typename A, typename L>
void cpu_relu(A* X, L N)
{
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    for (L i = 0; i < N; ++i) {
        if (X[i] <= 0.0) {
            X[i] = 0.0;
        }
    }
}

template <typename A, typename L>
void seq2col(A* output, const A* X, const L* lengths, L nW, L B, L I, L nL)
{
    // Let's say nW is 1 (it usually is). Then we want to take:

    // 1a 1b 1c
    // 2a 2b 2c
    // 3a 3b 3c

    // And make

    // __ __ __ 1a 1b 1c 2a 2b 2c
    // 1a 1b 1c 2a 2b 2c 3a 3b 3c
    // 2a 2b 2c 3a 3b 3c __ __ __

    // Where __ is padding.

    // Now let's say nW is 2. Then we want to take:

    // 1a 1b 1c
    // 2a 2b 2c
    // 3a 3b 3c

    // And make

    // __ __ __ __ __ __ 1a 1b 1c 2a 2b 2c 3a 3b 3c
    // __ __ __ 1a 1b 1c 2a 2b 2c 3a 3b 3c __ __ __
    // 1a 1b 1c 2a 2b 2c 3a 3b 3c __ __ __ __ __ __

    // * x_start=-6, x_end=9 : (0-2) * 3, (0+2+1) * 3
    // * x_start=-3, x_end=13 : (1-2) * 3, (1+2+1) * 3
    // * x_start=0, x_end=16 : (2-2) * 3, (2+2+1) * 3

    // If lengths > 1, then the sequence lengths dictate
    // the boundaries/padding rather than the begin/end
    // of X.
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    L nF = nW * 2 + 1;

    L seq_start = 0;
    for (L i = 0; i < nL; ++i) {
        // Calculate the bounds of the next sequence.
        L seq_end = seq_start + lengths[i];

        for (L j = seq_start; j < seq_end; ++j) {
            // Find the unconstrained window around b, which
            // may be out of the sequence bounds.
            L window_start = j - nW;
            L window_end = j + nW + 1;

            // Find the sequence-constrained window around b.
            L x_start = std::max(seq_start, window_start);
            L x_end = std::min(seq_end, window_end);
            L n_elems = x_end - x_start;

            L out_offset = x_start - window_start;

            std::memcpy(output + (j * nF * I) + (out_offset * I),
                X + (x_start * I),
                n_elems * I * sizeof(*output));
        }

        seq_start += lengths[i];
    }
}

template <typename A, typename L>
void backprop_seq2col(A* d_seqs, const A* d_cols, const L* lengths, L B, L I, L nW, L nL)
{
    // here's what we're doing, if we had 2d indexing.
    // for i in range(b):
    //     d_seq[i] += d_cols[i-2, 4]
    //     d_seq[i] += d_cols[i-1, 3]
    //     d_seq[i] += d_cols[i, 2]
    //     d_seq[i] += d_cols[i+1, 1]
    //     d_seq[i] += d_cols[i+2, 0]
    static_assert(std::is_floating_point<A>::value,
        "Array should be floating point");
    static_assert(std::is_integral<L>::value, "Array length should be integral");

    L nF = nW * 2 + 1;

    L seq_start = 0;
    for (L i = 0; i < nL; ++i) {
        // Calculate the bounds of the next sequence.
        L seq_end = seq_start + lengths[i];

        for (L j = seq_start; j < seq_end; ++j) {
            // Find the unconstrained window around b, which
            // may be out of the sequence bounds.
            L window_begin = j - nW;
            L window_end = j + nW + 1;

            // Find the sequence-constrained window around b.
            L d_seqs_begin = std::max(seq_start, window_begin);
            L d_seqs_end = std::min(seq_end, window_end);
            L n_elems = d_seqs_end - d_seqs_begin;

            // If the left window is cut short, we want to
            // start by the same amount in the output.
            L out_offset = d_seqs_begin - window_begin;

            vec_add(d_seqs + d_seqs_begin * I,
                d_cols + (j * nF * I) + (out_offset * I),
                static_cast<A>(1.), n_elems * I);
        }

        seq_start += lengths[i];
    }
}

template <typename F, typename I, typename L>
void cpu_gather_add(typename axpy<F>::ptr axpy, F* out_bo, const F* table_to, const I* indices_bk, L T, L O, L B, L K) {
     for (L b = 0; b < B; ++b) {
        for (L k = 0; k < K; ++k) {
            I idx = indices_bk[b * K + k];
            if (idx > T) {
                throw std::out_of_range("Embedding index out-of-bounds");
            }
            axpy(O, 1.0, table_to + idx * O, 1, out_bo + b * O, 1);
        }
    }
}


#endif // CPU_KERNELS_HH
