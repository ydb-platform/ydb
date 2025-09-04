#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

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

template <typename TSchemeProto>
class TSchemeDescriber {
public:
    const TSchemeProto& GetScheme() const {
        return Scheme;
    }

protected:
    TSchemeProto Scheme;
};

template <typename TPublicProto>
class TPublicDescriber {
public:
    const TPublicProto& GetPublic() const {
        return Public;
    }

    bool CompareWithString(const TString& str) const {
        TPublicProto proto;
        google::protobuf::TextFormat::ParseFromString(str, &proto);

        return Public.DebugString() == proto.DebugString();
    } 
protected:
    TPublicProto Public;
};

class TFileDescriber {
public:
    TFileDescriber(const TString& dir, const TString& name) 
                  : Dir(dir)
                  , Path(dir + name) {}

    const TString& GetDir() const {
        return Dir;
    }

    const TString& GetPath() const {
        return Path;
    }

protected:
    const TString Dir;
    const TString Path;
};

class TPermissions : public TPublicDescriber<Ydb::Scheme::ModifyPermissionsRequest>
                   , public TFileDescriber {
public:
    TPermissions(const TString& dir) : TFileDescriber(dir, "/permissions.pb") {
        google::protobuf::TextFormat::ParseFromString(
                                R"(actions {
                                    change_owner: "root@builtin"
                                })", &Public);
    }
};

template <typename TSchemeProto, typename TPublicProto>
class TObjectDescriber : public TSchemeDescriber<TSchemeProto>
                       , public TPublicDescriber<TPublicProto> {
public:
    template <typename TFormat>
    TFormat Get() const {return {};}

    template <>
    const TSchemeProto& Get<const TSchemeProto&>() const {
        return this->GetScheme();
    }

    template <>
    const TPublicProto& Get<const TPublicProto&>() const {
        return this->GetPublic();
    }
};

template <typename TSchemeProto, typename TPublicProto>
class TSchemeObjectDescriber : public TObjectDescriber<TSchemeProto, TPublicProto> 
                             , public TFileDescriber {
public:
    TSchemeObjectDescriber(const TString& dir, const TString& name) 
                  : TFileDescriber(dir, name)
                  , Permissions(dir) {
        ExportRequestItem = GetItemsFromTemp(ExportRequestItemTemp);
        ImportRequestItem = GetItemsFromTemp(ImportRequestItemTemp);
    }
    
    const TPermissions& GetPermissions() const {
        return Permissions;
    }
    
    const TString& GetExportRequestItem() const {
        return ExportRequestItem;
    }

    const TString& GetImportRequestItem() const {
        return ImportRequestItem;
    }

    TString GetRestoredDir() const {
        return Dir + "_restored";
    }
                  
protected:
    const TPermissions Permissions;
    TString ExportRequestItem;
    TString ImportRequestItem;

private:

    TString GetItemsFromTemp(const char* temp) {
        return Sprintf(temp, Dir.c_str(), Dir.c_str());
    }

    const char* ExportRequestItemTemp = R"(
        items {
            source_path: "/MyRoot%s"
            destination_prefix: "%s"
        }
    )";

    const char* ImportRequestItemTemp = R"(
        items {
            source_prefix: "%s"
            destination_path: "/MyRoot%s_restored"
        }
    )";
};

class TXxportRequest {
public:
    TXxportRequest(const char* type, const TVector<TString>& items, ui16 port) {
        Request = Sprintf(RequestTemp, type, Reduce(items).c_str(), port);
    }

    TXxportRequest(const char* type, const TVector<TString>& items) {
        Request = Sprintf(RequestTemp, type, Reduce(items).c_str());
        ui64 pos = Request.find("localhost:0");
        if (pos != std::string::npos) {
            Request.erase(pos, 11);
            Request.insert(pos, "localhost:%d");
        }
    }

    const TString& GetRequest() const {
        return Request;
    }

    static TString Reduce(const TVector<TString>& items) {
        return std::accumulate(
            items.begin(), items.end(), TString{},
            [](const TString& acc, const auto& item) {
                return acc + item + "\n";
            }
        );
    }

private:
    TString Request;
    const char* RequestTemp = R"(
        %sSettings {
            %s
            endpoint: "localhost:%d"
            scheme: HTTP
        }
    )";
};

class TExportRequest : public TXxportRequest {
public:
    TExportRequest(ui16 port, const TVector<TString>& items)
                  : TXxportRequest("ExportToS3", items, port) {}
    
    TExportRequest(const TVector<TString>& items)
                  : TXxportRequest("ExportToS3", items) {}
};

class TImportRequest : public TXxportRequest {
public:
    TImportRequest(ui16 port, const TVector<TString>& items)
                  : TXxportRequest("ImportFromS3", items, port) {}
    
    TImportRequest(const TVector<TString>& items)
                  : TXxportRequest("ImportFromS3", items) {}
};

class TTopic : public TSchemeObjectDescriber<NKikimrSchemeOp::TPersQueueGroupDescription,
                                             Ydb::Topic::CreateTopicRequest> {
private:
class TConsumer : public TObjectDescriber<NKikimrPQ::TPQTabletConfig::TConsumer,
                                          Ydb::Topic::Consumer> {
public:
    TConsumer(ui64 number, bool important = false) {
        google::protobuf::TextFormat::ParseFromString(Sprintf(ConsumerScheme, number), &Scheme);
        google::protobuf::TextFormat::ParseFromString(Sprintf(ConsumerPublic, number), &Public);
        Scheme.SetImportant(important);
        if (important) 
            Public.set_important(important);
    }

private:
    const char* ConsumerScheme = R"(
        Name: "Consumer_%d"
    )";

    const char* ConsumerPublic = R"(
        name: "Consumer_%d"
        read_from {
        }
        attributes {
            key: "_service_type"
            value: "data-streams"
        }
    )";
};

public:
    TTopic(ui64 number, ui64 countConsumers = 0) 
       : TSchemeObjectDescriber(Sprintf("/Topic_%d", number), "/create_topic.pb") {
        google::protobuf::TextFormat::ParseFromString(Sprintf(TopicScheme, number), &Scheme);
        google::protobuf::TextFormat::ParseFromString(TopicPublic, &Public);

        for (ui64 i = 0; i < countConsumers; ++i) {
            auto consumer = TConsumer(i, i % 2);
            *Scheme.MutablePQTabletConfig()->AddConsumers() = consumer.GetScheme();
            *Public.mutable_consumers()->Add() = consumer.GetPublic();
        }
    }

    const ::google::protobuf::RepeatedPtrField<Ydb::Topic::Consumer>& GetConsumers() const {
        return Public.consumers();
    }

    TString GetCorruptedPublicFile() const {
        Ydb::Topic::CreateTopicRequest corrupted = this->GetPublic();
        google::protobuf::Duration duration;
        duration.set_seconds(-1);
        duration.set_nanos(0);
        *corrupted.mutable_retention_period() = duration;
        return corrupted.DebugString();
    }

private:
    const char* TopicScheme = R"(
        Name: "Topic_%d"
        TotalGroupCount: 2
        PartitionPerTablet: 1
        PQTabletConfig {
            PartitionConfig {
                LifetimeSeconds: 10
            }
        }
    )";

    const char* TopicPublic = R"(
        partitioning_settings {
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
    )";
};

} //NDesc
