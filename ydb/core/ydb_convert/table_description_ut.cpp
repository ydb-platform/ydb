#include "table_description.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/strip.h>

#include <optional>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ConvertTableDescription) {
    Y_UNIT_TEST(ColumnCompressionRoundTrip) {
        using TCompression = Ydb::Table::ColumnCompression;
        for (const auto algorithm : {TCompression::ALGORITHM_OFF, TCompression::ALGORITHM_LZ4, TCompression::ALGORITHM_ZSTD}) {
            for (const auto level : {std::optional<int>{}, std::optional<int>{0}, std::optional<int>{-5}, std::optional<int>{5}}) {
                if (level && algorithm != TCompression::ALGORITHM_ZSTD) {
                    continue;
                }
                Ydb::Table::CreateTableRequest original;
                auto* column = original.add_columns();
                column->set_name("value");
                column->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
                column->mutable_compression()->set_algorithm(algorithm);
                if (level) {
                    column->mutable_compression()->set_compression_level(*level);
                }
                auto* defaultColumn = original.add_columns();
                defaultColumn->set_name("default_compression");
                *defaultColumn->mutable_type() = column->type();

                NKikimrSchemeOp::TColumnTableDescription internal;
                auto status = Ydb::StatusIds::SUCCESS;
                TString error;
                UNIT_ASSERT_C(FillColumnDescription(internal, original.columns(), status, error), error);

                Ydb::Table::CreateTableRequest exported;
                FillColumnDescription(exported, internal);
                Ydb::Table::DescribeTableResult described;
                FillColumnDescription(described, internal);
                for (const auto* restored : {&exported.columns(), &described.columns()}) {
                    UNIT_ASSERT_VALUES_EQUAL(restored->size(), 2);
                    const auto& compression = restored->Get(0).compression();
                    UNIT_ASSERT(compression.has_algorithm());
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(compression.algorithm()), static_cast<int>(algorithm));
                    UNIT_ASSERT_VALUES_EQUAL(compression.has_compression_level(), level.has_value());
                    if (level) {
                        UNIT_ASSERT_VALUES_EQUAL(compression.compression_level(), *level);
                    }
                    UNIT_ASSERT(!restored->Get(1).has_compression());
                }
            }
        }
    }

    template <typename T>
    struct TInvoker {
        T Value;

        template <typename F, typename... Args>
        explicit TInvoker(F&& f, Args&&... args)
            : Value(std::invoke(std::forward<F>(f), std::forward<Args>(args)...))
        { }

        T Result() {
            return Value;
        }
    };

    template <>
    struct TInvoker<void> {
        template <typename F, typename... Args>
        explicit TInvoker(F&& f, Args&&... args) {
            std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
        }

        void Result() {}
    };

    template <typename TOut, typename TIn, typename R, typename... Args>
    using TConvertFn = R(*)(TOut&, const TIn&, Args...);

    template <typename TIn, typename TOut, typename R = void, typename... Args>
    auto Test(TConvertFn<TOut, TIn, R, Args...>&& fn, const TString& input, const TString& output, Args&&... args) {
        TIn in;
        google::protobuf::TextFormat::ParseFromString(input, &in);

        TOut out;
        auto ret = TInvoker<R>(fn, out, in, std::forward<Args>(args)...);

        TString result;
        google::protobuf::TextFormat::PrintToString(out, &result);

        UNIT_ASSERT_NO_DIFF(StripInPlace(result), Strip(output));
        ret.Result();
    }

    Y_UNIT_TEST(StorageSettings) {
        // no default family
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 1
    StorageConfig {
      SysLog {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: false
      }
    }
  }
})", R"(
)");

        // non-strict storage pool
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      SysLog {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: true
      }
    }
  }
})", R"(
storage_settings {
  store_external_blobs: DISABLED
}
)");

        // strict storage pools
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      SysLog {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: false
      }
      Log {
        PreferredPoolKind: "ssd"
        AllowOtherKinds: false
      }
    }
  }
})", R"(
storage_settings {
  tablet_commit_log0 {
    media: "ssd"
  }
  tablet_commit_log1 {
    media: "ssd"
  }
  store_external_blobs: DISABLED
}
)");

        // external blobs
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillStorageSettings, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      External {
        PreferredPoolKind: "hdd"
        AllowOtherKinds: false
      }
      ExternalThreshold: 1
      ExternalChannelsCount: 7
    }
  }
})", R"(
storage_settings {
  external {
    media: "hdd"
  }
  store_external_blobs: ENABLED
  external_data_channels_count: 7
}
)");
    }

    Y_UNIT_TEST(ColumnFamilies) {
        // default family, explicit id: 0
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 0
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
}
)");

        // default family, without id & name
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
}
)");

        // named family
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Name: "foo"
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "foo"
  compression: COMPRESSION_NONE
}
)");

        // identified family
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Id: 1
    StorageConfig {
      Data {
        PreferredPoolKind: "ssd"
      }
    }
  }
})", R"(
column_families {
  name: "<id: 1>"
  compression: COMPRESSION_NONE
}
)");

        // strict storage pool
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    StorageConfig {
      Data {
        PreferredPoolKind: "hdd"
        AllowOtherKinds: false
      }
    }
  }
})", R"(
column_families {
  name: "default"
  data {
    media: "hdd"
  }
  compression: COMPRESSION_NONE
}
)");

        // compression
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    ColumnCodec: ColumnCodecLZ4
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_LZ4
}
)");

        // legacy compression
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    Codec: 1
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_LZ4
}
)");

        // caching
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    ColumnCache: ColumnCacheEver
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
  keep_in_memory: ENABLED
}
)");

        // legacy caching
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    InMemory: true
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
  keep_in_memory: ENABLED
}
)");

        // cache mode
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(&FillColumnFamilies, R"(
PartitionConfig {
  ColumnFamilies {
    ColumnCacheMode: ColumnCacheModeTryKeepInMemory
  }
})", R"(
column_families {
  name: "default"
  compression: COMPRESSION_NONE
  cache_mode: CACHE_MODE_IN_MEMORY
}
)");
    }

    using TFillIndexDescFn = void(*)(Ydb::Table::DescribeTableResult&, const NKikimrSchemeOp::TTableDescription&);

    Y_UNIT_TEST(IndexDescriptionLocalBloomFilter) {
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(
            static_cast<TFillIndexDescFn>(&FillIndexDescription), R"(
TableIndexes {
  Name: "bloom_idx"
  Type: EIndexTypeLocalBloomFilter
  State: EIndexStateReady
  KeyColumnNames: "data"
  BloomFilterDescription {
    FalsePositiveProbability: 0.05
  }
})", R"(
indexes {
  name: "bloom_idx"
  index_columns: "data"
  status: STATUS_READY
  local_bloom_filter_index {
    false_positive_probability: 0.05
  }
})");
    }

    Y_UNIT_TEST(IndexDescriptionLocalBloomNgramFilter) {
        Test<NKikimrSchemeOp::TTableDescription, Ydb::Table::DescribeTableResult>(
            static_cast<TFillIndexDescFn>(&FillIndexDescription), R"(
TableIndexes {
  Name: "ngram_idx"
  Type: EIndexTypeLocalBloomNgramFilter
  State: EIndexStateReady
  KeyColumnNames: "text_col"
  BloomNGrammFilterDescription {
    NGrammSize: 3
    HashesCount: 2
    FilterSizeBytes: 512
    RecordsCount: 1024
    CaseSensitive: true
  }
})", R"(
indexes {
  name: "ngram_idx"
  index_columns: "text_col"
  status: STATUS_READY
  local_bloom_ngram_filter_index {
    ngram_size: 3
    case_sensitive: true
  }
})");
    }

} // ConvertTableDescription

} // NKikimr
