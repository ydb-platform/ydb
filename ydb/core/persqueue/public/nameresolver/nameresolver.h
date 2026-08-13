#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <expected>
#include <optional>

namespace NKikimr::NPQ::NNameResolver {

/**
 * Converts a topic name to its full path.
 * Also converts a full/legacy topic name to the full path used for reading from mirrored topics.
 *
 * Returns resolved path on success, or error reason on failure.
 *
 * First-class citizen (FCC) mode:
 *   Converts legacy-style names (rt3.*, short legacy with --/@, or bare name without '/').
 *   Modern paths with '/' are joined with database via NormalizePath (full topic path).
 *
 * Federation mode (!FCC):
 *   - Root-like database (empty, prefixes PQ Root, or prefixes LbUserDatabaseRoot):
 *     path with '/' is federation account/topic under LbRoot; explicit legacy (rt3/--/@)
 *     also under LbRoot; bare names stay under the request database.
 *   - User database (under LbRoot, e.g. …/account): relative modern path inside that database
 *     (already mirrored …-mirrored-from-<dc> names are kept as-is).
 *
 * Root for resolved modern paths is LbUserDatabaseRoot from PQConfig, or database if Lb root is empty
 * (for user-database federation paths the request database is preferred).
 *
 * Full paths under database are accepted: the database prefix is stripped before resolving
 * (e.g. database "/Root" + name "/Root/account/topic" → same as name "account/topic").
 *
 * If dc is empty (default), it is taken from an rt3.<dc>--... name when present;
 * for short / modern paths without rt3., empty dc falls back to localDc.
 * Empty dc and localDc (both default) means local (no -mirrored-from- suffix)
 * for short and modern names.
 *
 * Examples (LbRoot = "/Root/LbCommunal", PQ Root = "/Root/PQ"):
 *
 *   // Without localDc/dc (defaults): local path, no -mirrored-from- suffix
 *   ResolveName(db, "rt3.dc1--account--topic")
 *     -> "/Root/LbCommunal/account/topic"
 *   ResolveName("/Root", "account/topic")
 *     -> "/Root/LbCommunal/account/topic"
 *   ResolveName("/Root", "/Root/account/topic")
 *     -> "/Root/LbCommunal/account/topic"
 *   ResolveName("/Root/LbCommunal/account", "dir/topic")
 *     -> "/Root/LbCommunal/account/dir/topic"
 *
 *   // With localDc (mirroring / DC-aware resolve)
 *   ResolveName(db, "rt3.dc1--account--topic", "dc1")
 *     -> "/Root/LbCommunal/account/topic"
 *   ResolveName("/Root", "account/topic", "dc1", "dc2")
 *     -> "/Root/LbCommunal/account/topic-mirrored-from-dc2"
 */
std::expected<TString, TString> ResolveName(
    TStringBuf database,
    TStringBuf name,
    TStringBuf localDc = {},
    TStringBuf dc = {}
);

struct TFederationAccountTarget {
    TString Path;            // absolute under federationRoot
    TString AccountDatabase; // federationRoot/account
};

/**
 * If path is under federationRoot and has account/... shape, returns SchemeCache target
 * (same path + account database). Otherwise nullopt.
 */
std::optional<TFederationAccountTarget> TryFederationAccountTarget(
    TStringBuf path,
    TStringBuf federationRoot
);

} // namespace NKikimr::NPQ::NNameResolver
