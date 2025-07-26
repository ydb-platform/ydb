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
  external_data_channels_count: 1
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
  external_data_channels_count: 1
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
    }
  }
})", R"(
storage_settings {
  external {
    media: "hdd"
  }
  store_external_blobs: ENABLED
  external_data_channels_count: 1
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
    }

} // ConvertTableDescription

} // NKikimr
