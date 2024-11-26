#pragma once

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

}