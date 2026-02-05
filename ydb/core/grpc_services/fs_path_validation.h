#pragma once

#include <util/generic/string.h>

namespace NKikimr::NGRpcService {

/**
 * Validates filesystem path for security vulnerabilities using current platform conventions.
 *
 * Platform-aware validation
 * Checks for:
 * - Null bytes
 * - Path traversal sequences (..)
 * - Current directory references (.)
 *
 * @param path The path to validate
 * @param pathDescription Human-readable description of the path (for error messages)
 * @param error Output parameter for error message if validation fails
 * @return true if path is valid, false otherwise
 */
bool ValidateFsPath(const TString& path, const TString& pathDescription, TString& error);

} // namespace NKikimr::NGRpcService
