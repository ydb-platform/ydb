#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NSchemeShardUT_Private::NLocalIndexes {

inline TString OlapTableWithBloomAndNgramIndexes(const TString& tableName) {
    return TStringBuilder() << R"(
            Name: ")" << tableName << R"("
            ColumnShardCount: 1
            Schema {
                Columns { Name: "timestamp" Type: "Timestamp" NotNull: true }
                Columns { Name: "resource_id" Type: "Utf8" }
                Columns { Name: "uid" Type: "Utf8" NotNull: true }
                KeyColumnNames: "timestamp"
                KeyColumnNames: "uid"
                Indexes {
                    Id: 1
                    Name: "idx_bloom"
                    ClassName: "BLOOM_FILTER"
                    BloomFilter {
                        FalsePositiveProbability: 0.01
                        ColumnIds: 2
                    }
                }
                Indexes {
                    Id: 2
                    Name: "idx_ngram"
                    ClassName: "BLOOM_NGRAMM_FILTER"
                    BloomNGrammFilter {
                        NGrammSize: 3
                        FalsePositiveProbability: 0.01
                        CaseSensitive: true
                        ColumnId: 2
                    }
                }
            }
        )";
}

}   // namespace NSchemeShardUT_Private::NLocalIndexes
