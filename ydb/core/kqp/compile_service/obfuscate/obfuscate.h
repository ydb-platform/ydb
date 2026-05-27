#pragma once

#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_tablemetadata.pb.h>

#include <yql/essentials/public/issue/yql_issue.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp {

// Obfuscates query replay messages by replacing user-defined identifiers
// (table names, column names, index names) with type-prefixed counters
// (table_0, column_0, index_0, etc.).
//
// Usage:
//   1. Call AddTableMetadata() for each metadata proto to build the mapping.
//   2. Call ObfuscateMetadata() to get obfuscated copies of metadata protos.
//   3. Call ObfuscateQueryText() to obfuscate query SQL using the same mapping.
//   Or use ObfuscateReplayMessage() for a full JSON round-trip.
class TReplayMessageObfuscator {
public:
    TReplayMessageObfuscator();

    // Phase 1: Scan metadata to build the identifier mapping.
    // Call once for each table metadata proto.
    void AddTableMetadata(const NKikimrKqp::TKqpTableMetadataProto& meta);

    // Phase 2a: Produce an obfuscated copy of a metadata proto.
    // Must be called after AddTableMetadata() has been called for all tables.
    NKikimrKqp::TKqpTableMetadataProto ObfuscateMetadata(
        const NKikimrKqp::TKqpTableMetadataProto& meta) const;

    // Phase 2b: Obfuscate query text using the built mapping via an AST-based visitor.
    // Returns true on success.
    bool ObfuscateQueryText(const TString& queryText, TString& result,
                            NYql::TIssues& issues) const;

    // Convenience: obfuscate a full replay JSON message (metadata + query text).
    // Returns true on success.
    bool ObfuscateReplayMessage(const TString& replayJson, TString& result,
                                NYql::TIssues& issues);

    // Add an explicit mapping entry (e.g., for database path, cluster).
    void AddMapping(const TString& original, const TString& obfuscated);

    // Get the current identifier mapping (original -> obfuscated) for testing/inspection.
    const THashMap<TString, TString>& GetMapping() const;

private:
    TString GetOrCreateTableName(const TString& original);
    TString GetOrCreateColumnName(const TString& original);
    TString GetOrCreateIndexName(const TString& original);

    // Look up an identifier in the mapping. Returns empty string if not found.
    TString MapIdentifier(const TString& original) const;

    void AddTableMetadataImpl(const NKikimrKqp::TKqpTableMetadataProto& meta);

    THashMap<TString, TString> Mapping_;
    ui32 TableCounter_ = 0;
    ui32 ColumnCounter_ = 0;
    ui32 IndexCounter_ = 0;
};

} // namespace NKikimr::NKqp
