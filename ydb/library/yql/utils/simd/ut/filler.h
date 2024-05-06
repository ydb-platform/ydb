#pragma once

#include <util/system/types.h>
#include <vector>
#include <cstddef>
#include <utility>
#include <cstdint>
#include <cstring>
#include <numeric>


class ByteFiller {
public:
    explicit ByteFiller(const std::vector<size_t>& sizes, size_t length)
        : m_sizes(sizes)
        , m_length(length) {
    }

    /*
    Fill SoA buffers like this:
    data[0] = <0, 1, ..., Size1 - 1, 0, 1, ..., Size1 - 1, ...>
    data[1] = <Size1, Size + 1, ..., Size1 + Size2 - 1, Size1, Size + 1, ..., Size1 + Size2 - 1, ...>
    ...

    For example, if Size1 == Size2 == Size3 == Size4 == 4:
    data[0] = <0,  1,  2,  3,  0,  1,  2,  3, ...>
    data[1] = <4,  5,  6,  7,  4,  5,  6,  7, ...>
    data[2] = <8,  9,  10, 11, 8,  9,  10, 11, ...>
    data[3] = <12, 13, 14, 15, 12, 13, 14, 15, ...>
    */
    void FillCols(i8** data) const {
        size_t cols_count = m_sizes.size();
        size_t acc = 0;
        for (size_t col_i = 0; col_i < cols_count; ++col_i) {
            i8* column = data[col_i];
            for (size_t i = 0; i < m_length; ++i) {
                size_t next_byte = acc;
                for (size_t byte_i = 0; byte_i < m_sizes[col_i]; ++byte_i) {
                    std::memcpy(column + m_sizes[col_i] * i + byte_i, &next_byte, 1);
                    next_byte++;
                }
            }
            acc += m_sizes[col_i];
        }
    }

    /*
    Fill AoS buffer like this:
    data[result] = <0, 1, ..., Size1 + Size2 + Size 3 + Size 4 - 1, 0, 1, ..., Size1 + Size2 + Size 3 + Size 4 - 1, ...>

    For example, if Size1 == Size2 == Size3 == Size4 == 4:
    result = <0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, ...>
    */    
    void FillRows(i8* result) const {
        size_t acc = std::accumulate(m_sizes.begin(), m_sizes.end(), 0);
        for (size_t i = 0; i < m_length; ++i) {
            for (size_t byte_i = 0; byte_i < acc; ++byte_i) {
                std::memcpy(result + acc * i + byte_i, &byte_i, 1);
            }
        }
    }

    // Compare byte order
    bool CompareRows(i8* result) const {
        size_t acc = std::accumulate(m_sizes.begin(), m_sizes.end(), 0);
        for (size_t i = 0; i < m_length; ++i) {
            for (size_t byte_n = 0; byte_n < acc; ++byte_n) {
                if (static_cast<size_t>(result[i * acc + byte_n]) != byte_n) {
                    return false;
                }
            }
        }
        return true;
    }

private:
    std::vector<size_t> m_sizes;
    size_t              m_length;
};
