#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

using EDataFormat = NKikimr::NDataShard::NBackupRestoreTraits::EDataFormat;
using ECompressionCodec = NKikimr::NDataShard::NBackupRestoreTraits::ECompressionCodec;

#define Y_UNIT_TEST_WITH_COMPRESSION(N)                                                                                               \
    template<ECompressionCodec Codec> void N(NUnitTest::TTestContext&);                                                               \
    struct TTestRegistration##N {                                                                                                     \
        TTestRegistration##N() {                                                                                                      \
            TCurrentTest::AddTest(#N "[Raw]",  static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ECompressionCodec::None>), false);  \
            TCurrentTest::AddTest(#N "[Zstd]", static_cast<void (*)(NUnitTest::TTestContext&)>(&N<ECompressionCodec::Zstd>),  false); \
        }                                                                                                                             \
    };                                                                                                                                \
    static TTestRegistration##N testRegistration##N;                                                                                  \
    template<ECompressionCodec Codec>                                                                                                 \
    void N(NUnitTest::TTestContext&)

namespace NAttr {

enum class EKeys {
    TOPIC_DESCRIPTION,
};

class TAttributes : public THashMap<EKeys, TString> {
public:
    const TString& GetTopicDescription() const {
        return this->at(EKeys::TOPIC_DESCRIPTION);
    }

};
} // NAttr

struct TTypedScheme {
    NKikimrSchemeOp::EPathType Type;
    TString Scheme;
    NAttr::TAttributes Attributes;

    TTypedScheme(const char* scheme)
        : Type(NKikimrSchemeOp::EPathTypeTable)
        , Scheme(scheme)
    {}

    TTypedScheme(const TString& scheme)
        : Type(NKikimrSchemeOp::EPathTypeTable)
        , Scheme(scheme)
    {}

    TTypedScheme(NKikimrSchemeOp::EPathType type, TString scheme)
        : Type(type)
        , Scheme(std::move(scheme))
    {}

    TTypedScheme(NKikimrSchemeOp::EPathType type, TString scheme, NAttr::TAttributes&& attributes)
        : Type(type)
        , Scheme(std::move(scheme))
        , Attributes(std::move(attributes))
    {}
};

namespace NDescUT {

class TSchemeFormat : public TString {};
class TPublicFormat : public TString {};

struct TFormatsDesc {
    TSchemeFormat Scheme;
    TPublicFormat Public;
};

class IDescriber {
public:
    virtual const TSchemeFormat& GetSheme() const = 0;
    virtual const TPublicFormat& GetPublic() const = 0;

    template <typename TFormat>
    TFormat Get() const {return {};}

    template <> 
    virtual const TSchemeFormat& Get<const TSchemeFormat&>() const {
        return GetSheme();
    }

    template <> 
    virtual const TPublicFormat& Get<const TPublicFormat&>() const {
        return GetPublic();
    }

    virtual const TFormatsDesc& GetFormatsDesc() const {
        return FormatsDesc;
    }

    static TString Reduce(const TVector<TString>& items) {
        return std::accumulate(
            items.begin(), items.end(), TString{},
            [](const TString& acc, const auto& item) {
                return acc + item + "\n";
            }
        );
    }

    template <typename TProto>
    static void CompareStringsAsProto(const TString& left, const TString& right) {
        TProto protoLeft;
        google::protobuf::TextFormat::ParseFromString(left, &protoLeft);

        TProto protoRight;
        google::protobuf::TextFormat::ParseFromString(right, &protoRight);

        UNIT_ASSERT_STRINGS_EQUAL(protoLeft.DebugString(), protoRight.DebugString());
    } 

    virtual ~IDescriber() = default;

private:
    TFormatsDesc FormatsDesc = {};
};

class TPermissions {
public:
    TPermissions(const TString& dir) : Path(dir + "/permissions.pb") {}
    const TString& GetPath() const {
        return Path;
    }

    const TString& GetContent() const {
        return Content;
    }

private:
    const TString Path;
    const TString Content = R"(actions {
  change_owner: "root@builtin"
}
)";
};

class TExportRequest {
public:
    TExportRequest(ui16 port, const TVector<TString>& items) {
        ExportRequest = Sprintf(ExportRequestTemp, port, IDescriber::Reduce(items).c_str());
    }

    const TString& GetRequest() const {
        return ExportRequest;
    }

private:
    TString ExportRequest;
    const char* ExportRequestTemp = R"(
        ExportToS3Settings {
            endpoint: "localhost:%d"
            scheme: HTTP
            %s
        }
    )";
};

class TTopic : public IDescriber {
private:
class TConsumer : public IDescriber {
public:
    TConsumer(ui64 number, bool important = false) {
        Scheme = {Sprintf(consumerScheme, number, important ? "true" : "false")};
        Public = {Sprintf(consumerPublic, number, important ? "\n  important: true" : "")};
    }

    const TSchemeFormat& GetSheme() const override {
        return Scheme;
    }

    const TPublicFormat& GetPublic() const override {
        return Public;
    }

private:
    TSchemeFormat Scheme;
    TPublicFormat Public;

    const char* consumerScheme = R"(
        Consumers {
        Name: "Consumer_%d"
        Important: %s
        }
    )";

    const char* consumerPublic = R"(consumers {
  name: "Consumer_%d"%s
  read_from {
  }
  attributes {
    key: "_service_type"
    value: "data-streams"
  }
})";
};

public:
    TTopic(ui64 number, ui64 countConsumers = 0) 
    : Dir(Sprintf("/Topic_%d", number))
    , Path(Dir + "/create_topic.pb")
    , Permissions(Dir) {
        for (ui64 i = 0; i < countConsumers; ++i) {
            Consumers.emplace_back(i, i % 2);
        }
        Scheme = {Sprintf(topicScheme, number, GetSchemeConsumers().c_str())};
        Public = {Sprintf(topicPublic, GetPublicConsumers().c_str())};
        RequestItem = Sprintf(requestItem, number, number);
    }

    const TSchemeFormat& GetSheme() const override {
        return Scheme;
    }

    const TPublicFormat& GetPublic() const override {
        return Public;
    }

    const TString& GetCreateTopicPath() const {
        return Path;
    }

    const TString& GetDir() const {
        return Dir;
    }

    const TPermissions& GetParmissions() const {
        return Permissions;
    }

    const TString& GetRequestItem() const {
        return RequestItem;
    }

private:
    const TString Dir;
    const TString Path;
    TPermissions Permissions;
    TSchemeFormat Scheme;
    TPublicFormat Public;
    TString RequestItem;
    TVector<TConsumer> Consumers;

    template <typename TFormat>
    TFormat GetConsumers() const {
        TFormat result;
        for (const auto& consumer : Consumers) {
            result += consumer.Get<TFormat>() + "\n";
        }
        return result;
    }

    TSchemeFormat GetSchemeConsumers() const {
        return GetConsumers<TSchemeFormat>();
    }

    TPublicFormat GetPublicConsumers() const {
        return GetConsumers<TPublicFormat>();
    }

    const char* topicScheme = R"(
        Name: "Topic_%d"
        TotalGroupCount: 2
        PartitionPerTablet: 1
        PQTabletConfig {
        PartitionConfig {
            LifetimeSeconds: 10
        }
        %s
        }
    )";

    const char* topicPublic = R"(partitioning_settings {
  min_active_partitions: 2
  max_active_partitions: 1
  auto_partitioning_settings {
    strategy: AUTO_PARTITIONING_STRATEGY_DISABLED
    partition_write_speed {
      stabilization_window {
        seconds: 300
      }
      up_utilization_percent: 80
      down_utilization_percent: 20
    }
  }
}
retention_period {
  seconds: 10
}
supported_codecs {
}
partition_write_speed_bytes_per_second: 50000000
partition_write_burst_bytes: 50000000
%s)";

    const char* requestItem = R"(
        items {
            source_path: "/MyRoot/Topic_%d"
            destination_prefix: "/Topic_%d"
        }
    )";

};

} //NDesc
