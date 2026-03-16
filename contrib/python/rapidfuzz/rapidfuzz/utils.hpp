/* SPDX-License-Identifier: MIT */
/* Copyright © 2020-present Max Bachmann */

#pragma once
#include <algorithm>
#include <array>
#include <cctype>
#include <cmath>
#include <cstddef>
#include <cwctype>
#include <limits>
#include <stdint.h>
#include <vector>

uint32_t UnicodeDefaultProcess(uint32_t ch);

/**
 * @brief removes any non alphanumeric characters, trim whitespaces from
 * beginning/end and lowercase the string. Currently this only supports
 * Ascii. Characters outside of the ascii spec are not changed. This
 * will be changed in the future to support full unicode. In case this has
 * has a noticeable effect on the performance an additional `ascii_default_process`
 * function will be provided, that keeps this behaviour
 *
 * @tparam CharT char type of the string
 *
 * @param s string to process
 *
 * @return returns the processed string
 */
template <typename CharT>
int64_t default_process(CharT* str, int64_t len)
{
    /* mapping converting
     * - non alphanumeric characters to whitespace (32)
     * - alphanumeric characters to lowercase
     *
     * generated using
     * `[ord(chr(x).lower()) if chr(x).isalnum() else 0x20 for x in range(256)]`
     * in Python3.9
     */
    static const int extended_ascii_mapping[256] = {
        32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,
        32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,
        32,  32,  32,  32,  32,  32,  32,  32,  48,  49,  50,  51,  52,  53,  54,  55,  56,  57,  32,  32,
        32,  32,  32,  32,  32,  97,  98,  99,  100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111,
        112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 32,  32,  32,  32,  32,  32,  97,  98,  99,
        100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
        120, 121, 122, 32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,
        32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  32,
        32,  32,  32,  32,  32,  32,  32,  32,  32,  32,  170, 32,  32,  32,  32,  32,  32,  32,  178, 179,
        32,  181, 32,  32,  32,  185, 186, 32,  188, 189, 190, 32,  224, 225, 226, 227, 228, 229, 230, 231,
        232, 233, 234, 235, 236, 237, 238, 239, 240, 241, 242, 243, 244, 245, 246, 32,  248, 249, 250, 251,
        252, 253, 254, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239,
        240, 241, 242, 243, 244, 245, 246, 32,  248, 249, 250, 251, 252, 253, 254, 255};

    std::transform(str, str + len, str, [](CharT ch) {
        /* irrelevant cases for a given char type are removed at compile time by any decent compiler
         */
        if (ch < 0 || ch > std::numeric_limits<uint32_t>::max())
            return ch;
        else if (ch < 256)
            return static_cast<CharT>(extended_ascii_mapping[ch]);
        else
            return static_cast<CharT>(UnicodeDefaultProcess(static_cast<uint32_t>(ch)));
    });

    while (len > 0 && str[len - 1] == ' ')
        len--;

    int64_t prefix = 0;
    while (len > 0 && str[prefix] == ' ') {
        len--;
        prefix++;
    }

    if (prefix != 0) std::copy(str + prefix, str + prefix + len, str);

    return len;
}

template <typename CharT>
std::vector<CharT> default_process_copy(CharT* str_, int64_t len_)
{
    std::vector<CharT> str(str_, str_ + len_);
    int64_t len = default_process(str.data(), str.size());
    str.resize(len);
    return str;
}
