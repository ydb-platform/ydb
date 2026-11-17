#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/library/table_digest/aggregate.h>

#include <library/cpp/resource/resource.h>

namespace NKikimr::NKqp::NDigestTest {

namespace NDigest = NYql::NDigest;

inline const TString& DigestFactorySql() {
    static const TString sql = [] {
        TString result = NResource::Find("digest_factory.sql");
        const auto exportPos = result.rfind("EXPORT $DigestFactory;");
        UNIT_ASSERT(exportPos != TString::npos);
        result.resize(exportPos);
        return result;
    }();
    return sql;
}

struct TDigestResult {
    ui32 Version = 0;
    TString Algorithm;
    ui64 Count = 0;
    TString Hash;
};

inline TDigestResult ReadDigest(NYdb::TValueParser& value) {
    TDigestResult digest;
    value.OpenStruct();
    while (value.TryNextMember()) {
        const auto& name = value.GetMemberName();
        if (name == "version") {
            digest.Version = value.GetUint32();
        } else if (name == "algorithm") {
            digest.Algorithm = value.GetUtf8();
        } else if (name == "count") {
            digest.Count = value.GetUint64();
        } else {
            UNIT_ASSERT_VALUES_EQUAL(name, "hash");
            digest.Hash = value.GetString();
        }
    }
    value.CloseStruct();
    return digest;
}

inline TDigestResult ReadDigest(const NYdb::TResultSet& result) {
    NYdb::TResultSetParser rows(result);
    UNIT_ASSERT(rows.TryNextRow());
    auto digest = ReadDigest(rows.ColumnParser(0));
    UNIT_ASSERT(!rows.TryNextRow());
    return digest;
}

inline TDigestResult ReadChecksum(const NYdb::TResultSet& result) {
    UNIT_ASSERT_VALUES_EQUAL(result.ColumnsCount(), 4);
    NYdb::TResultSetParser rows(result);
    UNIT_ASSERT(rows.TryNextRow());
    TDigestResult digest{
        .Version = rows.ColumnParser("version").GetUint32(),
        .Algorithm = TString(rows.ColumnParser("algorithm").GetUtf8()),
        .Count = rows.ColumnParser("count").GetUint64(),
        .Hash = TString(rows.ColumnParser("hash").GetString()),
    };
    UNIT_ASSERT(!rows.TryNextRow());
    return digest;
}

inline void AssertDigest(const TDigestResult& actual, const NDigest::TState& expected) {
    UNIT_ASSERT_VALUES_EQUAL(actual.Version, NDigest::TState::Version);
    UNIT_ASSERT_VALUES_EQUAL(actual.Algorithm, NDigest::GetAlgorithmName(expected.GetAlgorithm()));
    UNIT_ASSERT_VALUES_EQUAL(actual.Count, expected.GetCount());
    UNIT_ASSERT_VALUES_EQUAL(actual.Hash, expected.GetHashBytes());
}

} // namespace NKikimr::NKqp::NDigestTest
