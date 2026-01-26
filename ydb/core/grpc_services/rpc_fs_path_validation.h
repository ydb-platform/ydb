#pragma once

#include <util/generic/string.h>

namespace NKikimr {
namespace NGRpcService {

/**
 * Validates Unix-style filesystem path (with / separator) for security vulnerabilities.
 * 
 * Checks for:
 * - Path traversal sequences (..)
 * - Current directory references (.)
 * 
 * @param path The path to validate
 * @param pathDescription Human-readable description of the path (for error messages)
 * @param error Output parameter for error message if validation fails
 * @return true if path is valid, false otherwise
 */
bool ValidateUnixPath(const TString& path, const TString& pathDescription, TString& error);

/**
 * Validates Windows-style filesystem path (with \ separator) for security vulnerabilities.
 * 
 * Checks for:
 * - Path traversal sequences (..)
 * - Current directory references (.)
 * 
 * @param path The path to validate
 * @param pathDescription Human-readable description of the path (for error messages)
 * @param error Output parameter for error message if validation fails
 * @return true if path is valid, false otherwise
 */
bool ValidateWindowsPath(const TString& path, const TString& pathDescription, TString& error);

/**
 * Validates filesystem path for security vulnerabilities on both Unix and Windows platforms.
 * 
 * Checks for:
 * - Mixed path separators (both / and \ in the same path) - REJECTED
 * - Path traversal sequences (..) in Unix-style paths (/)
 * - Path traversal sequences (..) in Windows-style paths (\)
 * - Current directory references (.)
 * - Null bytes
 * 
 * The function first determines the path style (Unix or Windows) based on separators present,
 * rejects mixed-style paths, then applies appropriate validation rules.
 * This prevents cross-platform path confusion attacks.
 * 
 * @param path The path to validate
 * @param pathDescription Human-readable description of the path (for error messages)
 * @param error Output parameter for error message if validation fails
 * @return true if path is valid, false otherwise
 */
bool ValidateFsPath(const TString& path, const TString& pathDescription, TString& error);

} // namespace NGRpcService
} // namespace NKikimr
