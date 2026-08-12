#pragma once

#include <util/generic/strbuf.h>
#include <util/generic/string.h>

#include <expected>

namespace NKikimr::NPQ::NNameResolver {

/**
 * Converts a topic name to its full path.
 * Also converts a full/legacy topic name to the full path used for reading from mirrored topics.
 *
 * Returns resolved path on success, or error reason on failure
 * (same Valid/Reason semantics and texts as TDiscoveryConverter).
 *
 * First-class citizen (FCC) mode:
 *   Converts legacy-style names (rt3.*, short legacy with --/@, or bare name without '/').
 *   Modern paths with '/' are joined with database via NormalizePath (full topic path).
 *
 * Federation mode (!FCC) — mirrors TDiscoveryConverter::BuildForFederation:
 *   - Root / PQ database: path with '/' is federation account/topic;
 *     otherwise the name is treated as legacy (rt3. / short / bare).
 *   - User database: path is relative modern path inside that database.
 *
 * Root for resolved modern paths is LbUserDatabaseRoot from PQConfig, or database if Lb root is empty
 * (for user-database federation paths the request database is preferred).
 *
 * If dc is empty (default), it is taken from an rt3.<dc>--... name when present;
 * for short / modern paths without rt3., empty dc falls back to localDc.
 *
 * Examples (localDc = "dc1", LbRoot = "/Root/LbCommunal", PQ Root = "/Root/PQ"):
 *
 *   // FCC + local rt3
 *   ResolveName(db, "rt3.dc1--account--topic", "dc1")
 *     -> "/Root/LbCommunal/account/topic"
 *
 *   // FCC + remote rt3 — mirrored-from-<dc>
 *   ResolveName(db, "rt3.dc2--account--topic", "dc1")
 *     -> "/Root/LbCommunal/account/topic-mirrored-from-dc2"
 *
 *   // FCC + short legacy / bare
 *   ResolveName(db, "account--topic", "dc1")
 *     -> "/Root/LbCommunal/account/topic"
 *   ResolveName(db, "topic", "dc1", "dc2")
 *     -> "/Root/LbCommunal/topic-mirrored-from-dc2"
 *
 *   // FCC + modern path with '/' — full path under database
 *   ResolveName("/Root/db1", "dir/topic", "dc1")
 *     -> "/Root/db1/dir/topic"
 *   ResolveName("/Root/db1", "/dir/topic", "dc1")
 *     -> "/Root/db1/dir/topic"
 *
 *   // Federation + root DB + account/topic
 *   ResolveName("", "account/topic", "dc1")
 *     -> "/Root/LbCommunal/account/topic"
 *
 *   // Federation + user database + relative path
 *   ResolveName("/Root/LbCommunal/account", "dir/topic", "dc1")
 *     -> "/Root/LbCommunal/account/dir/topic"
 *
 *   // DC mismatch between rt3. name and dc argument — error
 *   ResolveName(db, "rt3.dc1--account--topic", "dc1", "dc2")
 *     -> unexpected("DC specified both in topic name and separate option and they mismatch. ")
 */
std::expected<TString, TString> ResolveName(
    TStringBuf database,
    TStringBuf name,
    TStringBuf localDc,
    TStringBuf dc = {}
);

} // namespace NKikimr::NPQ::NNameResolver
