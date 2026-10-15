#pragma once

#include <util/datetime/base.h>
#include <util/generic/buffer.h>
#include <util/generic/maybe.h>

namespace NKikimr::NDyNumber {

/**
 * DyNumber is a variable-length format to store large numbers.
 * Along with binary representation of DyNumber we declare string representation.
 * It lacks the original format properties but is human readable and can be passed to
 * other storage systems.
 */

/**
 * @brief Checks if buffer stores valid binary representation of DyNumber
 */
bool IsValidDyNumber(TStringBuf buffer);

/**
 * @brief Checks if buffer stores valid string representation of DyNumber
 */
bool IsValidDyNumberString(TStringBuf str);

/**
 * @brief Parses DyNumber string representation into binary one
 */
TMaybe<TString> ParseDyNumberString(TStringBuf str);

/**
 * @brief Converts DyNumber binary representation into string one
 */
TMaybe<TString> DyNumberToString(TStringBuf buffer);

/**
 * @brief Converts binary DyNumber to an instant, rounding up to microseconds
 * and clamping to [TInstant::Zero(), TInstant::Max()].
 * unitsInSecond must be 1, 1000, 1000000 or 1000000000.
 * Returns Nothing() for invalid binary data or unsupported units.
 */
TMaybe<TInstant> DyNumberToInstant(TStringBuf buffer, ui32 unitsInSecond);

} // namespace NKikimr::NDyNumber
