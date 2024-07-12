#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <util/string/strip.h>

using namespace NKikimr;
using namespace NSchemeShard;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(TSchemeShardInfoTypesTest) {

    void VerifyDeduplicateColumnFamiliesById(
            const TString& srcProtoText,
            const TString& dstProtoText,
            const TString& posMapExpected)
    {
        NKikimrSchemeOp::TPartitionConfig config;
        bool parseOk = ::google::protobuf::TextFormat::ParseFromString(srcProtoText, &config);
        UNIT_ASSERT_C(parseOk, "Failed to parse TPartitionConfig:\n" << srcProtoText);

        auto posById = TPartitionConfigMerger::DeduplicateColumnFamiliesById(config);

        TString result;
        ::google::protobuf::TextFormat::PrintToString(config, &result);
        TString expected = StripStringLeft(dstProtoText, [](const auto* it) { return *it == '\n'; });

        UNIT_ASSERT_VALUES_EQUAL(result, expected);

        TVector<ui32> ids;
        for (const auto& kv : posById) {
            ids.push_back(kv.first);
        }
        std::sort(ids.begin(), ids.end());

        TStringBuilder posMapResult;
        posMapResult << '{';
        bool first = true;
        for (ui32 id : ids) {
            if (first) {
                first = false;
            } else {
                posMapResult << ',';
            }
            posMapResult << ' ';
            posMapResult << id << " -> " << posById[id];
        }
        posMapResult << " }";
        UNIT_ASSERT_VALUES_EQUAL(TString(posMapResult), posMapExpected);
    }

    Y_UNIT_TEST(EmptyFamilies) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
}
)",
            R"(
ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0 }");
    }

    Y_UNIT_TEST(LostId) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(DeduplicationOrder) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

    Y_UNIT_TEST(MultipleDeduplications) {
        VerifyDeduplicateColumnFamiliesById(
            R"(ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "ssd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
            R"(
ColumnFamilies {
  Id: 0
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
ColumnFamilies {
  Id: 1
  StorageConfig {
    SysLog {
      PreferredPoolKind: "hdd"
    }
  }
}
)",
        "{ 0 -> 0, 1 -> 1 }");
    }

}
