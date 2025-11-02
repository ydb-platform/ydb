#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/tx/datashard/backup_restore_traits.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <contrib/libs/protobuf/src/google/protobuf/text_format.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/folder/path.h>
#include <util/string/builder.h>

#include <google/protobuf/util/message_differencer.h>

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

template <typename TPrivateProto>
class TPrivateProtoDescriber {
public:
    const TPrivateProto& GetPrivateProto() const {
        return PrivateProto;
    }

protected:
    TPrivateProto PrivateProto;
};

template <typename TPublicProto>
class TPublicProtoDescriber {
public:
    const TPublicProto& GetPublicProto() const {
        return PublicProto;
    }

    bool CompareWithString(const TString& str) const {
        TPublicProto proto;
        google::protobuf::TextFormat::ParseFromString(str, &proto);

        return ::google::protobuf::util::MessageDifferencer::Equals(PublicProto, proto);
    } 

protected:
    TPublicProto PublicProto;
};

class TFileDescriber {
public:
    TFileDescriber(const TFsPath& dir, const TFsPath& name) 
        : Dir(dir)
        , Path(dir / name) 
    {}

    const TString& GetDir() const {
        return Dir;
    }

    const TString& GetPath() const {
        return Path;
    }

protected:
    const TFsPath Dir;
    const TFsPath Path;
};

class TPermissions 
    : public TPublicProtoDescriber<Ydb::Scheme::ModifyPermissionsRequest>
    , public TFileDescriber
{
public:
    TPermissions(const TString& dir) 
        : TFileDescriber(dir, "permissions.pb")
    {
        google::protobuf::TextFormat::ParseFromString(
            R"(actions {
                change_owner: "root@builtin"
            })", 
            &PublicProto
        );
    }
};

template <typename TPrivateProto, typename TPublicProto>
class TObjectDescriber 
    : public TPrivateProtoDescriber<TPrivateProto>
    , public TPublicProtoDescriber<TPublicProto>
{
public:
    template <typename TFormat>
    TFormat Get() const {
        return {};
    }

    template <>
    const TPrivateProto& Get<const TPrivateProto&>() const {
        return this->GetPrivateProto();
    }

    template <>
    const TPublicProto& Get<const TPublicProto&>() const {
        return this->GetPublicProto();
    }
};

class TXxportRequest {
public:
    TXxportRequest(const char* type, const TVector<TString>& items) {
        Request = Sprintf(RequestTemp, type, Reduce(items).c_str());
        ui64 pos = Request.find("localhost:0");
        if (pos != TString::npos) {
            Request.erase(pos, 11);
            Request.insert(pos, "localhost:%d");
        }
    }

    TXxportRequest(const char* type, const TVector<TString>& items, ui16 port)
        : TXxportRequest(type, items) 
    {
        Request = Sprintf(Request.c_str(), port);
    }

    const TString& GetRequest() const {
        return Request;
    }

    static TString Reduce(const TVector<TString>& items) {
        return std::accumulate(
            items.begin(), items.end(), TString{},
            [](const TString& acc, const auto& item) {
                return TStringBuilder() << acc << item << "\n";
            }
        );
    }

private:
    TString Request;
    const char* RequestTemp = R"(
        %sSettings {
            %s
            endpoint: "localhost:0"
            scheme: HTTP
        }
    )";
};

class TExportRequest
    : public TXxportRequest 
{
public:
    TExportRequest(ui16 port, const TVector<TString>& items)
        : TXxportRequest("ExportToS3", items, port)
    {}
    
    TExportRequest(const TVector<TString>& items)
        : TXxportRequest("ExportToS3", items)
    {}
};

class TImportRequest 
    : public TXxportRequest
{
public:
    TImportRequest(ui16 port, const TVector<TString>& items)
        : TXxportRequest("ImportFromS3", items, port)
    {}
    
    TImportRequest(const TVector<TString>& items)
        : TXxportRequest("ImportFromS3", items)
    {}
};

template <typename TPrivateProto, typename TPublicProto>
class TSchemeObjectDescriber
    : public TObjectDescriber<TPrivateProto, TPublicProto> 
    , public TFileDescriber
{
public:
    TSchemeObjectDescriber(const TString& dir, const TString& name) 
        : TFileDescriber(dir, name)
        , Permissions(dir)
    {
        ExportRequestItem = GetItemsFromTemp(ExportRequestItemTemp);
        ImportRequestItem = GetItemsFromTemp(ImportRequestItemTemp);
        ExportRequest = NDescUT::TExportRequest({GetExportRequestItem()}).GetRequest();
        ImportRequest = NDescUT::TImportRequest({GetImportRequestItem()}).GetRequest();
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
        return TStringBuilder() << "/Restored" << Dir;
    }

    TString GetExportRequest(ui32 port) const {
        return NDescUT::TExportRequest(port, {GetExportRequestItem()}).GetRequest();
    }

    TString GetImportRequest(ui32 port) const {
        return NDescUT::TImportRequest(port, {GetImportRequestItem()}).GetRequest();
    }

    const TString& GetExportRequest() const {
        return ExportRequest;
    }

    const TString& GetImportRequest() const {
        return ImportRequest;
    }
                  
protected:
    const TPermissions Permissions;
    TString ExportRequestItem;
    TString ImportRequestItem;
    TString ExportRequest;
    TString ImportRequest;

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
            destination_path: "/MyRoot/Restored%s"
        }
    )";
};

class TSimpleTopic
    : public TSchemeObjectDescriber<NKikimrSchemeOp::TPersQueueGroupDescription, Ydb::Topic::CreateTopicRequest> 
{
private:
    class TSimpleConsumer 
        : public TObjectDescriber<NKikimrPQ::TPQTabletConfig::TConsumer, Ydb::Topic::Consumer> 
    {
    public:
        TSimpleConsumer(ui64 number, bool important = false) {
            google::protobuf::TextFormat::ParseFromString(Sprintf(ConsumerPrivate, number), &PrivateProto);
            google::protobuf::TextFormat::ParseFromString(Sprintf(ConsumerPublic, number), &PublicProto);
            PrivateProto.SetImportant(important);
            if (important) 
                PublicProto.set_important(important);
            PrivateProto.SetType(::NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);
            PublicProto.set_consumer_type(Ydb::Topic::CONSUMER_TYPE_STREAMING);
        }

    private:
        const char* ConsumerPrivate = R"(
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
    TSimpleTopic(ui64 number, ui64 countConsumers = 0) 
        : TSchemeObjectDescriber(Sprintf("/Topic_%d", number), "create_topic.pb")
    {
        google::protobuf::TextFormat::ParseFromString(Sprintf(TopicPrivate, number), &PrivateProto);
        google::protobuf::TextFormat::ParseFromString(TopicPublic, &PublicProto);

        for (ui64 i = 0; i < countConsumers; ++i) {
            auto consumer = TSimpleConsumer(i, i % 2);
            *PrivateProto.MutablePQTabletConfig()->AddConsumers() = consumer.GetPrivateProto();
            *PublicProto.mutable_consumers()->Add() = consumer.GetPublicProto();
        }
    }

    const ::google::protobuf::RepeatedPtrField<Ydb::Topic::Consumer>& GetConsumers() const {
        return PublicProto.consumers();
    }

    TString GetCorruptedPublicFile() const {
        Ydb::Topic::CreateTopicRequest corrupted = this->GetPublicProto();
        google::protobuf::Duration duration;
        duration.set_seconds(-1);
        duration.set_nanos(0);
        *corrupted.mutable_retention_period() = duration;
        return corrupted.DebugString();
    }

private:
    const char* TopicPrivate = R"(
        Name: "Topic_%d"
        TotalGroupCount: 1
        PartitionPerTablet: 1
        PQTabletConfig {
            PartitionConfig {
                LifetimeSeconds: 10
            }
        }
    )";

    const char* TopicPublic = R"(
        partitioning_settings {
            min_active_partitions: 1
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

} //NDescUT
