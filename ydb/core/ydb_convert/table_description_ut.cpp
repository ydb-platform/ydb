#include "table_description.h"

#include <google/protobuf/text_format.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/strip.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(ConvertTableDescription) {
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

    Y_UNIT_TEST(HnswIndexDescriptionRoundTrip) {
        for (const bool prefixed : {false, true}) {
            Ydb::Table::CreateTableRequest request;
            auto* index = request.add_indexes();
            index->set_name("vidx");
            if (prefixed) {
                index->add_index_columns("scope");
            }
            index->add_index_columns("embedding");
            auto* tree = index->mutable_global_hnsw_index()->mutable_vector_settings();
            tree->set_levels(1);
            tree->set_clusters(1);
            auto* vector = tree->mutable_settings();
            vector->set_metric(Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT);
            vector->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
            vector->set_vector_dimension(200);
            vector->set_min_rows(0);
            vector->set_m(24);
            vector->set_ef_construction(300);
            vector->set_delta_rows(1ULL << 40);

            NKikimrSchemeOp::TIndexedTableCreationConfig converted;
            Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
            TString error;
            UNIT_ASSERT_C(FillIndexDescription(converted, request, false, status, error), error);
            UNIT_ASSERT_VALUES_EQUAL(converted.IndexDescriptionSize(), 1);
            const auto& internal = converted.GetIndexDescription(0);
            UNIT_ASSERT_VALUES_EQUAL(internal.GetType(), NKikimrSchemeOp::EIndexTypeGlobalHnsw);
            UNIT_ASSERT_VALUES_EQUAL(internal.IndexImplTableDescriptionsSize(), prefixed ? 3 : 2);

            NKikimrSchemeOp::TTableDescription table;
            auto* stored = table.AddTableIndexes();
            stored->SetName(internal.GetName());
            stored->SetType(internal.GetType());
            stored->SetState(NKikimrSchemeOp::EIndexStateReady);
            *stored->MutableKeyColumnNames() = internal.GetKeyColumnNames();
            *stored->MutableVectorIndexKmeansTreeDescription() = internal.GetVectorIndexKmeansTreeDescription();
            *stored->MutableIndexImplTableDescriptions() = internal.GetIndexImplTableDescriptions();
            Ydb::Table::DescribeTableResult described;
            FillIndexDescription(described, table);
            UNIT_ASSERT_VALUES_EQUAL(described.indexes_size(), 1);
            UNIT_ASSERT(described.indexes(0).has_global_hnsw_index());
            UNIT_ASSERT_VALUES_EQUAL(described.indexes(0).global_hnsw_index().vector_settings().SerializeAsString(),
                tree->SerializeAsString());
        }
    }

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
