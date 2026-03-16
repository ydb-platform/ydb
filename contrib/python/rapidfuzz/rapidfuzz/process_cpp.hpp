#pragma once
#include <chrono>
#include "cpp_common.hpp"
#include "rapidfuzz.h"
#include "taskflow/taskflow.hpp"
#include "taskflow/algorithm/for_each.hpp"
#include <atomic>
#include <exception>
#include <stdexcept>
#include <vector>
using namespace std::chrono_literals;

template <typename T>
struct ListMatchElem {
    ListMatchElem()
    {}
    ListMatchElem(T score, int64_t index, PyObjectWrapper choice)
        : score(score), index(index), choice(std::move(choice))
    {}

    T score;
    int64_t index;
    PyObjectWrapper choice;
};

template <typename T>
struct DictMatchElem {
    DictMatchElem()
    {}
    DictMatchElem(T score, int64_t index, PyObjectWrapper choice, PyObjectWrapper key)
        : score(score), index(index), choice(std::move(choice)), key(std::move(key))
    {}

    T score;
    int64_t index;
    PyObjectWrapper choice;
    PyObjectWrapper key;
};

struct DictStringElem {
    DictStringElem() : index(-1)
    {}
    DictStringElem(int64_t index, PyObjectWrapper key, PyObjectWrapper val, RF_StringWrapper proc_val)
        : index(index), key(std::move(key)), val(std::move(val)), proc_val(std::move(proc_val))
    {}

    int64_t index;
    PyObjectWrapper key;
    PyObjectWrapper val;
    RF_StringWrapper proc_val;
};

struct ListStringElem {
    ListStringElem() : index(-1)
    {}
    ListStringElem(int64_t index, PyObjectWrapper val, RF_StringWrapper proc_val)
        : index(index), val(std::move(val)), proc_val(std::move(proc_val))
    {}

    int64_t index;
    PyObjectWrapper val;
    RF_StringWrapper proc_val;
};

struct ExtractComp {
    ExtractComp() : m_scorer_flags(nullptr)
    {}

    explicit ExtractComp(const RF_ScorerFlags* scorer_flags) : m_scorer_flags(scorer_flags)
    {}

    template <typename T>
    bool operator()(T const& a, T const& b) const
    {
        if (m_scorer_flags->flags & RF_SCORER_FLAG_RESULT_F64) {
            return is_first(a, b, m_scorer_flags->optimal_score.f64, m_scorer_flags->worst_score.f64);
        }
        if (m_scorer_flags->flags & RF_SCORER_FLAG_RESULT_SIZE_T) {
            return is_first(a, b, m_scorer_flags->optimal_score.sizet, m_scorer_flags->worst_score.sizet);
        }
        else {
            return is_first(a, b, m_scorer_flags->optimal_score.i64, m_scorer_flags->worst_score.i64);
        }
    }

private:
    template <typename T, typename U>
    static bool is_first(T const& a, T const& b, U optimal, U worst)
    {
        if (optimal > worst) {
            if (a.score > b.score) {
                return true;
            }
            else if (a.score < b.score) {
                return false;
            }
        }
        else {
            if (a.score > b.score) {
                return false;
            }
            else if (a.score < b.score) {
                return true;
            }
        }
        return a.index < b.index;
    }

    const RF_ScorerFlags* m_scorer_flags;
};

struct RF_ScorerWrapper {
    RF_ScorerFunc scorer_func;

    RF_ScorerWrapper() : scorer_func({nullptr, {nullptr}, nullptr})
    {}
    explicit RF_ScorerWrapper(RF_ScorerFunc scorer_func_) : scorer_func(scorer_func_)
    {}

    RF_ScorerWrapper(const RF_ScorerWrapper&) = delete;
    RF_ScorerWrapper& operator=(const RF_ScorerWrapper&) = delete;

    RF_ScorerWrapper(RF_ScorerWrapper&& other) : scorer_func(other.scorer_func)
    {
        other.scorer_func = {nullptr, {nullptr}, nullptr};
    }

    RF_ScorerWrapper& operator=(RF_ScorerWrapper&& other)
    {
        if (&other != this) {
            if (scorer_func.dtor) {
                scorer_func.dtor(&scorer_func);
            }

            scorer_func = other.scorer_func;
            other.scorer_func = {nullptr, {nullptr}, nullptr};
        }
        return *this;
    };

    ~RF_ScorerWrapper()
    {
        if (scorer_func.dtor) {
            scorer_func.dtor(&scorer_func);
        }
    }

    void call(const RF_String* str, double score_cutoff, double score_hint, double* result) const
    {
        PyErr2RuntimeExn(scorer_func.call.f64(&scorer_func, str, 1, score_cutoff, score_hint, result));
    }

    void call(const RF_String* str, int64_t score_cutoff, int64_t score_hint, int64_t* result) const
    {
        PyErr2RuntimeExn(scorer_func.call.i64(&scorer_func, str, 1, score_cutoff, score_hint, result));
    }

    void call(const RF_String* str, size_t score_cutoff, size_t score_hint, size_t* result) const
    {
        PyErr2RuntimeExn(scorer_func.call.sizet(&scorer_func, str, 1, score_cutoff, score_hint, result));
    }
};

struct RF_UncachedScorerWrapper {
    RF_UncachedScorerFunc scorer_func;

    explicit RF_UncachedScorerWrapper(RF_UncachedScorerFunc scorer_func_) : scorer_func(scorer_func_)
    {}

    RF_UncachedScorerWrapper(const RF_UncachedScorerWrapper&) = delete;
    RF_UncachedScorerWrapper& operator=(const RF_UncachedScorerWrapper&) = delete;

    RF_UncachedScorerWrapper(RF_UncachedScorerWrapper&& other) : scorer_func(other.scorer_func)
    {}

    RF_UncachedScorerWrapper& operator=(RF_UncachedScorerWrapper&& other)
    {
        if (&other != this) scorer_func = other.scorer_func;

        return *this;
    };

    void call(const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs, double score_cutoff,
              double score_hint, double* result) const
    {
        PyErr2RuntimeExn(scorer_func.call.f64(str1, str2, kwargs, score_cutoff, score_hint, result));
    }

    void call(const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs, int64_t score_cutoff,
              int64_t score_hint, int64_t* result) const
    {
        PyErr2RuntimeExn(scorer_func.call.i64(str1, str2, kwargs, score_cutoff, score_hint, result));
    }

    void call(const RF_String* str1, const RF_String* str2, const RF_Kwargs* kwargs, size_t score_cutoff,
              size_t score_hint, size_t* result) const
    {
        PyErr2RuntimeExn(scorer_func.call.sizet(str1, str2, kwargs, score_cutoff, score_hint, result));
    }
};

template <typename T>
bool is_lowest_score_worst(const RF_ScorerFlags* scorer_flags)
{
    if (std::is_same<T, double>::value) {
        return scorer_flags->optimal_score.f64 > scorer_flags->worst_score.f64;
    }
    else if (std::is_same<T, size_t>::value) {
        return scorer_flags->optimal_score.sizet > scorer_flags->worst_score.sizet;
    }
    else {
        return scorer_flags->optimal_score.i64 > scorer_flags->worst_score.i64;
    }
}

template <typename T>
T get_optimal_score(const RF_ScorerFlags* scorer_flags)
{
    if (std::is_same<T, double>::value) {
        return (T)scorer_flags->optimal_score.f64;
    }
    else if (std::is_same<T, size_t>::value) {
        return (T)scorer_flags->optimal_score.sizet;
    }
    else {
        return (T)scorer_flags->optimal_score.i64;
    }
}

template <typename T>
std::vector<DictMatchElem<T>> extract_dict_impl(const RF_Kwargs* kwargs, const RF_ScorerFlags* scorer_flags,
                                                RF_Scorer* scorer, const RF_StringWrapper& query,
                                                const std::vector<DictStringElem>& choices, T score_cutoff,
                                                T score_hint)
{
    std::vector<DictMatchElem<T>> results;
    results.reserve(choices.size());

    RF_ScorerFunc scorer_func;
    PyErr2RuntimeExn(scorer->scorer_func_init(&scorer_func, kwargs, 1, &query.string));
    RF_ScorerWrapper ScorerFunc(scorer_func);

    bool lowest_score_worst = is_lowest_score_worst<T>(scorer_flags);

    for (size_t i = 0; i < choices.size(); ++i) {
        if (i % 1000 == 0)
            if (PyErr_CheckSignals() != 0) throw std::runtime_error("");

        T score;
        ScorerFunc.call(&choices[i].proc_val.string, score_cutoff, score_hint, &score);

        if (lowest_score_worst) {
            if (score >= score_cutoff) {
                results.emplace_back(score, choices[i].index, choices[i].val, choices[i].key);
            }
        }
        else {
            if (score <= score_cutoff) {
                results.emplace_back(score, choices[i].index, choices[i].val, choices[i].key);
            }
        }
    }

    return results;
}

template <typename T>
std::vector<ListMatchElem<T>> extract_list_impl(const RF_Kwargs* kwargs, const RF_ScorerFlags* scorer_flags,
                                                RF_Scorer* scorer, const RF_StringWrapper& query,
                                                const std::vector<ListStringElem>& choices, T score_cutoff,
                                                T score_hint)
{
    std::vector<ListMatchElem<T>> results;
    results.reserve(choices.size());

    RF_ScorerFunc scorer_func;
    PyErr2RuntimeExn(scorer->scorer_func_init(&scorer_func, kwargs, 1, &query.string));
    RF_ScorerWrapper ScorerFunc(scorer_func);

    bool lowest_score_worst = is_lowest_score_worst<T>(scorer_flags);

    for (size_t i = 0; i < choices.size(); ++i) {
        if (i % 1000 == 0)
            if (PyErr_CheckSignals() != 0) throw std::runtime_error("");

        T score;
        ScorerFunc.call(&choices[i].proc_val.string, score_cutoff, score_hint, &score);

        if (lowest_score_worst) {
            if (score >= score_cutoff) {
                results.emplace_back(score, choices[i].index, choices[i].val);
            }
        }
        else {
            if (score <= score_cutoff) {
                results.emplace_back(score, choices[i].index, choices[i].val);
            }
        }
    }

    return results;
}

int64_t any_round(double score)
{
    return std::llround(score);
}

int64_t any_round(int64_t score)
{
    return score;
}

size_t any_round(size_t score)
{
    return score;
}

enum class MatrixType {
    UNDEFINED,
    FLOAT32,
    FLOAT64,
    INT8,
    INT16,
    INT32,
    INT64,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
};

struct Matrix {
    MatrixType m_dtype;
    size_t m_rows;
    size_t m_cols;
    void* m_matrix;

    Matrix() : m_dtype(MatrixType::FLOAT32), m_rows(0), m_cols(0), m_matrix(nullptr)
    {}

    Matrix(MatrixType dtype, size_t rows, size_t cols) : m_dtype(dtype), m_rows(rows), m_cols(cols)
    {

        m_matrix = malloc(get_dtype_size() * m_rows * m_cols);
        if (m_matrix == nullptr) throw std::bad_alloc();
    }

    Matrix(const Matrix& other) : m_dtype(other.m_dtype), m_rows(other.m_rows), m_cols(other.m_cols)
    {
        m_matrix = malloc(get_dtype_size() * m_rows * m_cols);
        if (m_matrix == nullptr) throw std::bad_alloc();

        memcpy(m_matrix, other.m_matrix, get_dtype_size() * m_rows * m_cols);
    }

    Matrix(Matrix&& other) noexcept : m_dtype(MatrixType::FLOAT32), m_rows(0), m_cols(0), m_matrix(nullptr)
    {
        other.swap(*this);
    }

    Matrix& operator=(Matrix other)
    {
        other.swap(*this);
        return *this;
    }

    void swap(Matrix& rhs) noexcept
    {
        using std::swap;
        swap(m_rows, rhs.m_rows);
        swap(m_cols, rhs.m_cols);
        swap(m_dtype, rhs.m_dtype);
        swap(m_matrix, rhs.m_matrix);
    }

    ~Matrix()
    {
        free(m_matrix);
    }

    int get_dtype_size()
    {
        switch (m_dtype) {
        case MatrixType::FLOAT32: return 4;
        case MatrixType::FLOAT64: return 8;
        case MatrixType::INT8: return 1;
        case MatrixType::INT16: return 2;
        case MatrixType::INT32: return 4;
        case MatrixType::INT64: return 8;
        case MatrixType::UINT8: return 1;
        case MatrixType::UINT16: return 2;
        case MatrixType::UINT32: return 4;
        case MatrixType::UINT64: return 8;
        default: throw std::invalid_argument("invalid dtype");
        }
    }

    const char* get_format()
    {
        switch (m_dtype) {
        case MatrixType::FLOAT32: return "f";
        case MatrixType::FLOAT64: return "d";
        case MatrixType::INT8: return "b";
        case MatrixType::INT16: return "h";
        case MatrixType::INT32: return "i";
        case MatrixType::INT64: return "q";
        case MatrixType::UINT8: return "B";
        case MatrixType::UINT16: return "H";
        case MatrixType::UINT32: return "I";
        case MatrixType::UINT64: return "Q";
        default: throw std::invalid_argument("invalid dtype");
        }
    }

    template <typename T>
    void set(size_t row, size_t col, T score)
    {
        void* data = (char*)m_matrix + get_dtype_size() * (row * m_cols + col);
        switch (m_dtype) {
        case MatrixType::FLOAT32: *((float*)data) = (float)score; break;
        case MatrixType::FLOAT64: *((double*)data) = (double)score; break;
        case MatrixType::INT8: *((int8_t*)data) = (int8_t)any_round(score); break;
        case MatrixType::INT16: *((int16_t*)data) = (int16_t)any_round(score); break;
        case MatrixType::INT32: *((int32_t*)data) = (int32_t)any_round(score); break;
        case MatrixType::INT64: *((int64_t*)data) = any_round(score); break;
        case MatrixType::UINT8: *((uint8_t*)data) = (uint8_t)any_round(score); break;
        case MatrixType::UINT16: *((uint16_t*)data) = (uint16_t)any_round(score); break;
        case MatrixType::UINT32: *((uint32_t*)data) = (uint32_t)any_round(score); break;
        case MatrixType::UINT64: *((uint64_t*)data) = (uint64_t)any_round(score); break;
        default: assert(false); break;
        }
    }
};

bool KeyboardInterruptOccured(PyThreadState*& save)
{
    PyEval_RestoreThread(save);
    bool res = PyErr_CheckSignals() != 0;
    save = PyEval_SaveThread();
    return res;
}

template <typename Func>
void run_parallel(int workers, int64_t rows, int64_t step_size, Func&& func)
{
    PyThreadState* save = PyEval_SaveThread();

    /* for these cases spawning threads causes to much overhead to be worth it */
    if (workers == 0 || workers == 1) {
        for (int64_t row = 0; row < rows; row += step_size) {
            if (KeyboardInterruptOccured(save)) {
                PyEval_RestoreThread(save);
                throw std::runtime_error("");
            }

            try {
                func(row, std::min(row + step_size, rows));
            }
            catch (...) {
                PyEval_RestoreThread(save);
                throw;
            }
        }

        PyEval_RestoreThread(save);
        return;
    }

    if (workers < 0) {
        workers = std::thread::hardware_concurrency();
    }

    std::exception_ptr exception = nullptr;
    std::atomic<int> exceptions_occurred{0};
    tf::Executor executor(workers);
    tf::Taskflow taskflow;

    taskflow.for_each_index((int64_t)0, rows, step_size, [&](int64_t row) {
        /* skip work after an exception occurred */
        if (exceptions_occurred.load() > 0) {
            return;
        }
        try {
            int64_t row_end = std::min(row + step_size, rows);
            func(row, row_end);
        }
        catch (...) {
            /* only store first exception */
            if (exceptions_occurred.fetch_add(1) == 0) {
                exception = std::current_exception();
            }
        }
    });

    auto future = executor.run(taskflow);
    while (future.wait_for(1s) != std::future_status::ready) {
        if (KeyboardInterruptOccured(save)) {
            exceptions_occurred.fetch_add(1);
            future.wait();
            PyEval_RestoreThread(save);
            /* exception already set */
            throw std::runtime_error("");
        }
    }
    PyEval_RestoreThread(save);

    if (exception) std::rethrow_exception(exception);
}

template <typename T>
static Matrix cdist_single_list_impl(const RF_ScorerFlags* scorer_flags, const RF_Kwargs* kwargs,
                                     RF_Scorer* scorer, const std::vector<RF_StringWrapper>& queries,
                                     MatrixType dtype, int workers, T score_cutoff, T score_hint,
                                     T score_multiplier, T worst_score)
{
    (void)scorer_flags;
    int64_t rows = queries.size();
    int64_t cols = queries.size();
    Matrix matrix(dtype, static_cast<size_t>(rows), static_cast<size_t>(cols));

    run_parallel(workers, rows, 1, [&](int64_t row, int64_t row_end) {
        for (; row < row_end; ++row) {
            RF_ScorerFunc scorer_func;
            PyErr2RuntimeExn(scorer->scorer_func_init(&scorer_func, kwargs, 1, &queries[row].string));
            RF_ScorerWrapper ScorerFunc(scorer_func);

            T score;
            if (queries[row].is_none())
                score = worst_score;
            else
                ScorerFunc.call(&queries[row].string, score_cutoff, score_hint, &score);

            matrix.set(row, row, score * score_multiplier);

            for (int64_t col = row + 1; col < cols; ++col) {
                if (queries[col].is_none())
                    score = worst_score;
                else
                    ScorerFunc.call(&queries[col].string, score_cutoff, score_hint, &score);

                matrix.set(row, col, score * score_multiplier);
                matrix.set(col, row, score * score_multiplier);
            }
        }
    });

    return matrix;
}

template <typename T>
static Matrix cdist_two_lists_impl(const RF_ScorerFlags* scorer_flags, const RF_Kwargs* kwargs,
                                   RF_Scorer* scorer, const std::vector<RF_StringWrapper>& queries,
                                   const std::vector<RF_StringWrapper>& choices, MatrixType dtype,
                                   int workers, T score_cutoff, T score_hint, T score_multiplier,
                                   T worst_score)
{
    int64_t rows = queries.size();
    int64_t cols = choices.size();
    Matrix matrix(dtype, static_cast<size_t>(rows), static_cast<size_t>(cols));
    bool multiStringInit = scorer_flags->flags & RF_SCORER_FLAG_MULTI_STRING_INIT;

    if (queries.empty() || choices.empty()) return matrix;

    if (multiStringInit) {
        std::vector<size_t> row_idx(rows);
        std::iota(row_idx.begin(), row_idx.end(), 0);
        auto none_begin = std::remove_if(row_idx.begin(), row_idx.end(), [&queries](size_t i) {
            return queries[i].is_none();
        });

        for (auto it = none_begin; it != row_idx.end(); it++)
            for (int64_t col = 0; col < cols; ++col)
                matrix.set(*it, col, worst_score * score_multiplier);

        row_idx.erase(none_begin, row_idx.end());

        /* all elements are None */
        if (row_idx.empty()) return matrix;

        /* sort into blocks fitting simd vectors */
        std::stable_sort(row_idx.begin(), row_idx.end(), [&queries](size_t i1, size_t i2) {
            size_t len1 = queries[i1].size();
            if (len1 <= 64)
                len1 = len1 / 8;
            else
                len1 = (64 / 8) + len1 / 64;

            size_t len2 = queries[i2].size();
            if (len2 <= 64)
                len2 = len2 / 8;
            else
                len2 = (64 / 8) + len2 / 64;

            return len1 > len2;
        });

        size_t smallest_size = queries[row_idx.back()].size();
        size_t step_size;
        if (smallest_size <= 8)
            step_size = 256 / 8;
        else if (smallest_size <= 16)
            step_size = 256 / 16;
        else if (smallest_size <= 32)
            step_size = 256 / 32;
        else
            step_size = 256 / 64;

        run_parallel(workers, row_idx.size(), step_size, [&](int64_t row, int64_t row_end) {
            /* todo add simd support for long sequences */
            for (; row < row_end; ++row) {
                if (queries[row_idx[row]].size() <= 64) break;

                RF_ScorerFunc scorer_func;
                PyErr2RuntimeExn(
                    scorer->scorer_func_init(&scorer_func, kwargs, 1, &queries[row_idx[row]].string));
                RF_ScorerWrapper ScorerFunc(scorer_func);

                for (int64_t col = 0; col < cols; ++col) {
                    T score;
                    if (choices[col].is_none())
                        score = worst_score;
                    else
                        ScorerFunc.call(&choices[col].string, score_cutoff, score_hint, &score);

                    matrix.set(row_idx[row], col, score * score_multiplier);
                }
            }

            int64_t row_count = row_end - row;
            if (row_count == 0) return;

            assert(row_count <= 256 / 8);
            T scores[256 / 8];
            RF_String strings[256 / 8];

            for (int64_t i = 0; i < row_count; ++i)
                strings[i] = queries[row_idx[row + i]].string;

            RF_ScorerFunc scorer_func;
            PyErr2RuntimeExn(scorer->scorer_func_init(&scorer_func, kwargs, row_count, strings));
            RF_ScorerWrapper ScorerFunc(scorer_func);

            for (int64_t col = 0; col < cols; ++col) {
                if (choices[col].is_none()) {
                    for (int64_t i = 0; i < row_count; ++i)
                        scores[i] = worst_score;
                }
                else {
                    ScorerFunc.call(&choices[col].string, score_cutoff, score_hint, scores);
                }

                for (int64_t i = 0; i < row_count; ++i)
                    matrix.set(row_idx[row + i], col, scores[i] * score_multiplier);
            }
        });
    }
    else {
        run_parallel(workers, rows, 1, [&](int64_t row, int64_t row_end) {
            for (; row < row_end; ++row) {
                if (queries[row].is_none()) {
                    for (int64_t col = 0; col < cols; ++col)
                        matrix.set(row, col, worst_score * score_multiplier);

                    continue;
                }

                RF_ScorerFunc scorer_func;
                PyErr2RuntimeExn(scorer->scorer_func_init(&scorer_func, kwargs, 1, &queries[row].string));
                RF_ScorerWrapper ScorerFunc(scorer_func);

                for (int64_t col = 0; col < cols; ++col) {
                    T score;
                    if (choices[col].is_none())
                        score = worst_score;
                    else
                        ScorerFunc.call(&choices[col].string, score_cutoff, score_hint, &score);

                    matrix.set(row, col, score * score_multiplier);
                }
            }
        });
    }

    return matrix;
}

template <typename T>
static Matrix cpdist_cpp_impl(const RF_Kwargs* kwargs, RF_Scorer* scorer,
                              const std::vector<RF_StringWrapper>& queries,
                              const std::vector<RF_StringWrapper>& choices, MatrixType dtype, int workers,
                              T score_cutoff, T score_hint, T score_multiplier, T worst_score)
{
    int64_t rows = queries.size();
    int64_t cols = 1;
    Matrix matrix(dtype, static_cast<size_t>(rows), static_cast<size_t>(cols));

    if (queries.empty() || choices.empty()) return matrix;

    // TODO: Adjust the batch size based on the query, choices & hardware available
    const int batchSize = 16;

    run_parallel(workers, rows, batchSize, [&](int64_t row, int64_t row_end) {
        for (; row < row_end; ++row) {
            T score;
            if (choices[row].is_none() || queries[row].is_none())
                score = worst_score;
            else
                RF_UncachedScorerWrapper(scorer->uncached_scorer_func)
                    .call(&queries[row].string, &choices[row].string, kwargs, score_cutoff, score_hint,
                          &score);

            matrix.set(row, 0, score * score_multiplier);
        }
    });

    return matrix;
}
