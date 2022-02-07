// The contents of this file is based on contents of:
//
// https://github.com/ulfjack/ryu/blob/master/ryu/common.h,
// https://github.com/ulfjack/ryu/blob/master/ryu/d2s.c, and
// https://github.com/ulfjack/ryu/blob/master/ryu/f2s.c,
//
// which are distributed under the following terms:
//--------------------------------------------------------------------------------
// Copyright 2018 Ulf Adams
//
// The contents of this file may be used under the terms of the Apache License,
// Version 2.0.
//
//    (See accompanying file LICENSE-Apache or copy at
//     http://www.apache.org/licenses/LICENSE-2.0)
//
// Alternatively, the contents of this file may be used under the terms of
// the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE-Boost or copy at
//     https://www.boost.org/LICENSE_1_0.txt)
//
// Unless required by applicable law or agreed to in writing, this software
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.
//--------------------------------------------------------------------------------
// Modifications Copyright 2020 Junekey Jeon
//
// Following modifications were made to the original contents:
//  - Put everything inside the namespace jkj::dragonbox::to_chars_detail
//  - Combined decimalLength9 (from common.h) and decimalLength17 (from d2s.c)
//    into a single template function decimal_length
//  - Combined to_chars (from f2s.c) and to_chars (from d2s.c) into a
//    single template function fp_to_chars_impl
//  - Removed index counting statements; replaced them with pointer increments
//  - Removed usages of DIGIT_TABLE; replaced them with radix_100_table
//
//  These modifications, together with other contents of this file may be used
//  under the same terms as the original contents.


#include "dragonbox/dragonbox_to_chars.h"

namespace jkj::dragonbox {
	namespace to_chars_detail {
		static constexpr char radix_100_table[] = {
			'0', '0', '0', '1', '0', '2', '0', '3', '0', '4',
			'0', '5', '0', '6', '0', '7', '0', '8', '0', '9',
			'1', '0', '1', '1', '1', '2', '1', '3', '1', '4',
			'1', '5', '1', '6', '1', '7', '1', '8', '1', '9',
			'2', '0', '2', '1', '2', '2', '2', '3', '2', '4',
			'2', '5', '2', '6', '2', '7', '2', '8', '2', '9',
			'3', '0', '3', '1', '3', '2', '3', '3', '3', '4',
			'3', '5', '3', '6', '3', '7', '3', '8', '3', '9',
			'4', '0', '4', '1', '4', '2', '4', '3', '4', '4',
			'4', '5', '4', '6', '4', '7', '4', '8', '4', '9',
			'5', '0', '5', '1', '5', '2', '5', '3', '5', '4',
			'5', '5', '5', '6', '5', '7', '5', '8', '5', '9',
			'6', '0', '6', '1', '6', '2', '6', '3', '6', '4',
			'6', '5', '6', '6', '6', '7', '6', '8', '6', '9',
			'7', '0', '7', '1', '7', '2', '7', '3', '7', '4',
			'7', '5', '7', '6', '7', '7', '7', '8', '7', '9',
			'8', '0', '8', '1', '8', '2', '8', '3', '8', '4',
			'8', '5', '8', '6', '8', '7', '8', '8', '8', '9',
			'9', '0', '9', '1', '9', '2', '9', '3', '9', '4',
			'9', '5', '9', '6', '9', '7', '9', '8', '9', '9'
		};

		template <class UInt>
		static constexpr std::uint32_t decimal_length(UInt const v) {
			if constexpr (std::is_same_v<UInt, std::uint32_t>) {
				// Function precondition: v is not a 10-digit number.
				// (f2s: 9 digits are sufficient for round-tripping.)
				// (d2fixed: We print 9-digit blocks.)
				assert(v < 1000000000);
				if (v >= 100000000) { return 9; }
				if (v >= 10000000) { return 8; }
				if (v >= 1000000) { return 7; }
				if (v >= 100000) { return 6; }
				if (v >= 10000) { return 5; }
				if (v >= 1000) { return 4; }
				if (v >= 100) { return 3; }
				if (v >= 10) { return 2; }
				return 1;
			}
			else {
				static_assert(std::is_same_v<UInt, std::uint64_t>);
				// This is slightly faster than a loop.
				// The average output length is 16.38 digits, so we check high-to-low.
				// Function precondition: v is not an 18, 19, or 20-digit number.
				// (17 digits are sufficient for round-tripping.)
				assert(v < 100000000000000000L);
				if (v >= 10000000000000000L) { return 17; }
				if (v >= 1000000000000000L) { return 16; }
				if (v >= 100000000000000L) { return 15; }
				if (v >= 10000000000000L) { return 14; }
				if (v >= 1000000000000L) { return 13; }
				if (v >= 100000000000L) { return 12; }
				if (v >= 10000000000L) { return 11; }
				if (v >= 1000000000L) { return 10; }
				if (v >= 100000000L) { return 9; }
				if (v >= 10000000L) { return 8; }
				if (v >= 1000000L) { return 7; }
				if (v >= 100000L) { return 6; }
				if (v >= 10000L) { return 5; }
				if (v >= 1000L) { return 4; }
				if (v >= 100L) { return 3; }
				if (v >= 10L) { return 2; }
				return 1;
			}
		}

		template <class Float>
		static char* to_chars_impl(unsigned_fp_t<Float> v, char* buffer)
		{
			auto output = v.significand;
			auto const olength = decimal_length(output);

			int32_t exp = v.exponent + (int32_t)olength - 1;

			if (exp >= -6 && exp <= 20)
			{
  				int index = 0;

				if (exp < 0)
				{
					buffer[index++] = '0';
					buffer[index++] = '.';

					while (++exp)
						buffer[index++] = '0';

					for (int32_t i = olength - 1; i >= 0; --i)
					{
						const uint32_t c = output % 10;
						output /= 10;
						buffer[index + i] = '0' + c;
					}
					index += olength;
				}
				else if (exp + 1 >= olength)
				{
					for (int32_t i = olength - 1; i >= 0; --i)
					{
						const uint32_t c = output % 10;
						output /= 10;
						buffer[index + i] = '0' + c;
					}
					index += olength;

					while (exp >= olength)
					{
						buffer[index++] = '0';
						--exp;
					}
				}
				else
				{
					for (int32_t i = olength; i > exp + 1; --i)
					{
						const uint32_t c = output % 10;
						output /= 10;
						buffer[index + i] = '0' + c;
					}
					
					buffer[index + exp + 1] = '.';

					for (int32_t i = exp; i >= 0; --i)
					{
						const uint32_t c = output % 10;
						output /= 10;
						buffer[index + i] = '0' + c;
					}

					index += olength + 1;
				}

				return buffer + index;
			}

			// Print the decimal digits.
			// The following code is equivalent to:
			// for (uint32_t i = 0; i < olength - 1; ++i) {
			//   const uint32_t c = output % 10; output /= 10;
			//   result[index + olength - i] = (char) ('0' + c);
			// }
			// result[index] = '0' + output % 10;

			uint32_t i = 0;
			if constexpr (sizeof(Float) == 8) {
				// We prefer 32-bit operations, even on 64-bit platforms.
				// We have at most 17 digits, and uint32_t can store 9 digits.
				// If output doesn't fit into uint32_t, we cut off 8 digits,
				// so the rest will fit into uint32_t.
				if ((output >> 32) != 0) {
					// Expensive 64-bit division.
					const uint64_t q = output / 100000000;
					uint32_t output2 = ((uint32_t)output) - 100000000 * ((uint32_t)q);
					output = q;

					const uint32_t c = output2 % 10000;
					output2 /= 10000;
					const uint32_t d = output2 % 10000;
					const uint32_t c0 = (c % 100) << 1;
					const uint32_t c1 = (c / 100) << 1;
					const uint32_t d0 = (d % 100) << 1;
					const uint32_t d1 = (d / 100) << 1;
					memcpy(buffer + olength - i - 1, radix_100_table + c0, 2);
					memcpy(buffer + olength - i - 3, radix_100_table + c1, 2);
					memcpy(buffer + olength - i - 5, radix_100_table + d0, 2);
					memcpy(buffer + olength - i - 7, radix_100_table + d1, 2);
					i += 8;
				}
			}

			auto output2 = (uint32_t)output;
			while (output2 >= 10000) {
#ifdef __clang__ // https://bugs.llvm.org/show_bug.cgi?id=38217
				const uint32_t c = output2 - 10000 * (output2 / 10000);
#else
				const uint32_t c = output2 % 10000;
#endif
				output2 /= 10000;
				const uint32_t c0 = (c % 100) << 1;
				const uint32_t c1 = (c / 100) << 1;
				memcpy(buffer + olength - i - 1, radix_100_table + c0, 2);
				memcpy(buffer + olength - i - 3, radix_100_table + c1, 2);
				i += 4;
			}
			if (output2 >= 100) {
				const uint32_t c = (output2 % 100) << 1;
				output2 /= 100;
				memcpy(buffer + olength - i - 1, radix_100_table + c, 2);
				i += 2;
			}
			if (output2 >= 10) {
				const uint32_t c = output2 << 1;
				// We can't use memcpy here: the decimal dot goes between these two digits.
				buffer[olength - i] = radix_100_table[c + 1];
				buffer[0] = radix_100_table[c];
			}
			else {
				buffer[0] = (char)('0' + output2);
			}

			// Print decimal point if needed.
			if (olength > 1) {
				buffer[1] = '.';
				buffer += olength + 1;
			}
			else {
				++buffer;
			}

			// Print the exponent.
			*buffer = 'e';
			++buffer;
			if (exp < 0) {
				*buffer = '-';
				++buffer;
				exp = -exp;
			}
			if constexpr (sizeof(Float) == 8) {
				if (exp >= 100) {
					const int32_t c = exp % 10;
					memcpy(buffer, radix_100_table + 2 * (exp / 10), 2);
					buffer[2] = (char)('0' + c);
					buffer += 3;
				}
				else if (exp >= 10) {
					memcpy(buffer, radix_100_table + 2 * exp, 2);
					buffer += 2;
				}
				else {
					*buffer = (char)('0' + exp);
					++buffer;
				}
			}
			else {
				if (exp >= 10) {
					memcpy(buffer, radix_100_table + 2 * exp, 2);
					buffer += 2;
				}
				else {
					*buffer = (char)('0' + exp);
					++buffer;
				}
			}

			return buffer;
		}
		
		char* to_chars(unsigned_fp_t<float> v, char* buffer) {
			return to_chars_impl(v, buffer);
		}
		char* to_chars(unsigned_fp_t<double> v, char* buffer) {
			return to_chars_impl(v, buffer);
		}
	}
}
