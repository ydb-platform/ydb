#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <expected>
#include <optional>

namespace NKikimr::NPQ::NNameResolver {

struct TResolvedName {
    TString Path;              // absolute topic path
    TString NavigateDatabase;  // SchemeCache DatabaseName hint (may be empty)
};

/**
 * Converts a topic name to its full path and SchemeCache database hint.
 * Also converts a full/legacy topic name to the full path used for reading from mirrored topics.
 *
 * Returns resolved path + navigate database on success, or error reason on failure.
 *
 * NavigateDatabase:
 *   - FCC (non-federation): request database (absolute)
 *   - Federation + path under LbRoot/account/...: LbRoot/account
 *   - Otherwise: request database (absolute), or empty if request database is empty
 *
 * First-class citizen (FCC / non-federation) mode:
 *   Names are joined with the request database as-is. Legacy forms (rt3.*, --, @)
 *   are not converted: a leaf like TestSchemeList--test-topic-1 is a literal name.
 *
 * Federation mode (!TopicsAreFirstClassCitizen):
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
 *   // FCC: literal names under the request database (-- is not a path separator)
 *   ResolveName(db, "TestSchemeList--test-topic-1")
 *     -> Path=db+"/TestSchemeList--test-topic-1", NavigateDatabase=db
 *   ResolveName(db, "account/topic")
 *     -> Path=db+"/account/topic", NavigateDatabase=db
 *
 *   // Federation: without localDc/dc (defaults) — local path, no -mirrored-from- suffix
 *   ResolveName("/Root", "account/topic")
 *     -> Path="/Root/LbCommunal/account/topic", NavigateDatabase="/Root/LbCommunal/account"
 *   ResolveName("/Root/LbCommunal/account", "dir/topic")
 *     -> Path="/Root/LbCommunal/account/dir/topic", NavigateDatabase="/Root/LbCommunal/account"
 *
 *   // Federation: with localDc (mirroring / DC-aware resolve)
 *   ResolveName("/Root", "rt3.dc1--account--topic", "dc1")
 *     -> Path="/Root/LbCommunal/account/topic", NavigateDatabase="/Root/LbCommunal/account"
 *   ResolveName("/Root", "account/topic", "dc1", "dc2")
 *     -> Path="/Root/LbCommunal/account/topic-mirrored-from-dc2", NavigateDatabase="/Root/LbCommunal/account"
 */
std::expected<TResolvedName, TString> ResolveName(
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
