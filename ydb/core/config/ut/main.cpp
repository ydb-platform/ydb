#include <ydb/core/config/utils/config_traverse.h>
#include <ydb/core/protos/config.pb.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/protobuf/json/util.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/libs/protobuf/src/google/protobuf/descriptor.h>

#include <util/string/join.h>

#include <unordered_set>

using namespace NProtoBuf;
using namespace NKikimr::NConfig;

struct TRequiredFieldInfo {
    TString FieldPath;
    TVector<ui64> FieldNumberPath;

    bool operator==(const TRequiredFieldInfo& other) const {
        return FieldPath == other.FieldPath && FieldNumberPath == other.FieldNumberPath;
    }
};

template<>
struct std::hash<TRequiredFieldInfo>
{
    std::size_t operator()(const TRequiredFieldInfo& s) const noexcept
    {
        size_t hash = THash<TString>()(s.FieldPath);

        for (auto number : s.FieldNumberPath) {
            hash = CombineHashes(hash, THash<ui64>()(number));
        }

        return hash;
    }
};

class TConfigDumper {
private:
    TString PrintLabel(const FieldDescriptor& field) const {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        switch (field.label()) {
            case FieldDescriptor::LABEL_REQUIRED:
                return UseColors ? (TString(colors.Red()) + "required" + colors.Reset())  : TString("required");
            case FieldDescriptor::LABEL_OPTIONAL:
                return "optional";
            case FieldDescriptor::LABEL_REPEATED:
                return "repeated";
            default:
                Y_ABORT("unexpected");
        }
    }

    TString Loop() const {
        NColorizer::TColors colors = NColorizer::AutoColors(Cout);
        return UseColors ? (TString(colors.Red()) + " <- loop" + colors.Reset()) : TString(" <- loop");
    }

    TString FieldName(const FieldDescriptor* field) const {
        TString name = field->name();
        if (UseYamlNames) {
            NProtobufJson::ToSnakeCaseDense(&name);
        }
        return name;
    }

    TString RootName() const {
        return UseYamlNames ? "app_config" : "AppConfig";
    }

    TString DescriptorName(const Descriptor* d) const {
        return UseFullyQualifiedTypes ? d->full_name() : d->name();
    }

    TString ConstructFieldPath(const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t from = 1, ssize_t to = -1) const {
        to = to == -1 ? fieldPath.size() : to;
        TVector<TString> path;
        path.push_back(TString("/") + RootName());
        for (ssize_t i = from; i < to; ++i) {
            TString fieldName = FieldName(fieldPath[i]) + (UsePrintArrays && fieldPath[i]->is_repeated() ? "[]" : "");
            path.push_back(fieldName);
        }
        path.push_back(FieldName(field));

        return JoinSeq("/", path);
    }

    TRequiredFieldInfo ConstructFullFieldPath(const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field) const {
        TRequiredFieldInfo info;

        TVector<TString> path;
        path.push_back(TString("/") + RootName());
        for (size_t i = 1; i < fieldPath.size(); ++i) {
            TString fieldName = FieldName(fieldPath[i]) + (UsePrintArrays && fieldPath[i]->is_repeated() ? "[]" : "");
            path.push_back(fieldName);
            info.FieldNumberPath.push_back(fieldPath[i]->number());
        }
        path.push_back(FieldName(field));
        info.FieldNumberPath.push_back(field->number());

        info.FieldPath = JoinSeq("/", path);

        return info;
    }

public:
    void PrintFullTree() const {
        Traverse([this](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            Y_UNUSED(fieldPath);
            std::cout << TString(typePath.size() * 4, ' ') <<
                    (field ? PrintLabel(*field) : "")
                    << " " << (d ? DescriptorName(d) : field->cpp_type_name())
                    << " " << (field ? FieldName(field) : RootName())
                    << " = " << (field ? field->number() : 0) << ";"
                    << (loop != -1 ? Loop() : "")  << std::endl;
        },
        NKikimrConfig::TAppConfig::default_instance().GetDescriptor());
    }

    void PrintLoops() const {
        Traverse([this](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            if (loop != -1) {
                std::cout << "Loop at \"" << ConstructFieldPath(fieldPath, field, 1, loop + 1) << "\", types chain: ";

                TVector<TString> path;
                for (size_t i = loop; i < typePath.size(); ++i) {
                    path.push_back(DescriptorName(typePath[i]));
                }
                path.push_back(DescriptorName(d));
                std::cout << JoinSeq(" -> ", path) << std::endl;
            }
        },
        NKikimrConfig::TAppConfig::default_instance().GetDescriptor());

    }

    TMap<const FileDescriptor*, TMap<const Descriptor*, TMap<const FieldDescriptor*, std::unordered_set<TRequiredFieldInfo>>>> DumpRequired() const {
        TMap<const FileDescriptor*, TMap<const Descriptor*, TMap<const FieldDescriptor*, std::unordered_set<TRequiredFieldInfo>>>> fields;
        Traverse([&fields, this](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            Y_UNUSED(d, loop);
            if (field && field->is_required()) {
                fields[typePath.back()->file()][typePath.back()][field].insert(ConstructFullFieldPath(fieldPath, field));
            }
        },
        NKikimrConfig::TAppConfig::default_instance().GetDescriptor());
        return fields;
    }

    void PrintRequired(const TMap<const FileDescriptor*, TMap<const Descriptor*, TMap<const FieldDescriptor*, std::unordered_set<TRequiredFieldInfo>>>>& fields) const {
        for (const auto& [file, typeToField] : fields) {
            for (const auto& [type, fieldToPaths] : typeToField) {
                for (const auto& [field, paths] : fieldToPaths) {
                    SourceLocation sl;
                    bool hasSourceInfo = field->GetSourceLocation(&sl);
                    std::cout << "Required field \"" << FieldName(field) << "\" in "
                              << DescriptorName(type) << " at $/" << file->name()
                              << ":" << (hasSourceInfo ? sl.start_line + 1 : 0)
                              << "\n    it can be accessed in config by following paths:" << std::endl;
                    for (const auto& path : paths) {
                        std::cout << TString(8, ' ') << path.FieldPath << std::endl;
                    }
                }
            }
        }
    }

    bool UseYamlNames = false;
    bool UsePrintArrays = false;
    bool UseColors = true;
    bool UseFullyQualifiedTypes = true;
};

Y_UNIT_TEST_SUITE(ConfigProto) {
    Y_UNIT_TEST(ForbidNewRequired) {
        TConfigDumper dumper;
        auto fields = dumper.DumpRequired();

        TSet<TString> allowedPaths = {
            "/AppConfig/AuthConfig/LdapAuthentication/BaseDn/BaseDn",
            "/AppConfig/AuthConfig/LdapAuthentication/BindDn/BindDn",
            "/AppConfig/AuthConfig/LdapAuthentication/BindPassword/BindPassword",
            "/AppConfig/QueryServiceConfig/Generic/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/YqlCore/Flags/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Activation/ByHour/Hour/Hour",
            "/AppConfig/QueryServiceConfig/S3/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/HiddenActivation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/QueryServiceConfig/Generic/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/QueryServiceConfig/S3/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/YqlCore/Flags/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/HiddenActivation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/QueryServiceConfig/S3/DefaultSettings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Name/Name",
            "/AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Name/Name",
            "/AppConfig/QueryServiceConfig/Generic/DefaultSettings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Name/Name",
            "/AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Activation/ByHour/Hour/Hour",
            "/AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Name/Name",
            "/AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Value/Value",
            "/AppConfig/QueryServiceConfig/Yt/DefaultSettings/Activation/ByHour/Hour/Hour",
            "/AppConfig/QueryServiceConfig/Yt/DefaultSettings/Activation/ByHour/Percentage/Percentage",
            "/AppConfig/QueryServiceConfig/Yt/DefaultSettings/Name/Name",
            "/AppConfig/QueryServiceConfig/Yt/DefaultSettings/Value/Value",
            "/AppConfig/QueryServiceConfig/Yt/RemoteFilePatterns/Pattern/Pattern",
            "/AppConfig/QueryServiceConfig/Yt/RemoteFilePatterns/Cluster/Cluster",
            "/AppConfig/QueryServiceConfig/Yt/RemoteFilePatterns/Path/Path",
            "/AppConfig/QueryServiceConfig/Generic/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Value/Value",
            "/AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Value/Value",
            "/AppConfig/QueryServiceConfig/S3/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Value/Value",
            "/AppConfig/FederatedQueryConfig/Gateways/YqlCore/Flags/Name/Name",
            "/AppConfig/FederatedQueryConfig/Gateways/YqlCore/QPlayerActivation/ByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/YqlCore/QPlayerActivation/ByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Path/Project/Project",
            "/AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Path/Cluster/Cluster",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/WithHiddenByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultAutoByHour/Hour/Hour",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/WithHiddenByHour/Percentage/Percentage",
            "/AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultAutoByHour/Percentage/Percentage",
            "/AppConfig/KQPConfig/Settings/Name/Name",
            "/AppConfig/KQPConfig/Settings/Value/Value",
            "/AppConfig/ActorSystemConfig/ServiceExecutor/ServiceName/ServiceName",
            "/AppConfig/ActorSystemConfig/ServiceExecutor/ExecutorId/ExecutorId",
            "/AppConfig/SqsConfig/AuthConfig/OauthToken/TokenFile/TokenFile",
            "/AppConfig/SqsConfig/AuthConfig/Jwt/JwtFile/JwtFile",
        };

        TSet<TVector<ui64>> allowedNumberPaths = {
            {30, 74, 3, 3}, // /AppConfig/AuthConfig/LdapAuthentication/BaseDn/BaseDn
            {30, 74, 4, 4}, // /AppConfig/AuthConfig/LdapAuthentication/BindDn/BindDn
            {30, 74, 5, 5}, // /AppConfig/AuthConfig/LdapAuthentication/BindPassword/BindPassword
            {73, 6, 100, 3, 2, 1, 1}, // /AppConfig/QueryServiceConfig/S3/DefaultSettings/Activation/ByHour/Hour/Hour
            {73, 6, 1, 100, 3, 2, 1, 1}, // /AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Activation/ByHour/Hour/Hour
            {58, 9, 10, 6, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Activation/ByHour/Hour/Hour
            {58, 9, 6, 2, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Activation/ByHour/Hour/Hour
            {58, 9, 9, 1, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/YqlCore/Flags/Activation/ByHour/Hour/Hour
            {58, 9, 5, 100, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Activation/ByHour/Hour/Hour
            {58, 9, 5, 1, 100, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Activation/ByHour/Hour/Hour
            {73, 11, 6, 3, 2, 1, 1}, // /AppConfig/QueryServiceConfig/Generic/DefaultSettings/Activation/ByHour/Hour/Hour
            {58, 9, 4, 4, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Activation/ByHour/Hour/Hour
            {58, 9, 4, 1, 100, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Activation/ByHour/Hour/Hour
            {58, 9, 6, 1, 100, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Activation/ByHour/Hour/Hour
            {58, 9, 2, 9, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/HiddenActivation/ByHour/Hour/Hour
            {58, 9, 3, 100, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Activation/ByHour/Hour/Hour
            {58, 9, 3, 1, 100, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Activation/ByHour/Hour/Hour
            {58, 9, 2, 102, 3, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Activation/ByHour/Hour/Hour
            {73, 11, 6, 3, 2, 2, 2}, // /AppConfig/QueryServiceConfig/Generic/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {73, 6, 100, 3, 2, 2, 2}, // /AppConfig/QueryServiceConfig/S3/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {73, 6, 1, 100, 3, 2, 2, 2}, // /AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage
            {58, 9, 10, 6, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {58, 9, 5, 100, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {58, 9, 9, 1, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/YqlCore/Flags/Activation/ByHour/Percentage/Percentage
            {58, 9, 5, 1, 100, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage
            {58, 9, 4, 4, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {58, 9, 2, 102, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {58, 9, 4, 1, 100, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage
            {58, 9, 6, 2, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {58, 9, 6, 1, 100, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage
            {58, 9, 3, 100, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {58, 9, 3, 1, 100, 3, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage
            {58, 9, 2, 9, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/HiddenActivation/ByHour/Percentage/Percentage
            {73, 11, 6, 1, 1}, // /AppConfig/QueryServiceConfig/Generic/DefaultSettings/Name/Name
            {73, 6, 100, 1, 1}, // /AppConfig/QueryServiceConfig/S3/DefaultSettings/Name/Name
            {58, 9, 10, 6, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Name/Name
            {58, 9, 4, 1, 100, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Name/Name
            {73, 6, 1, 100, 1, 1}, // /AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Name/Name
            {58, 9, 6, 1, 100, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Name/Name
            {58, 9, 5, 100, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Name/Name
            {58, 9, 6, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Name/Name
            {58, 9, 5, 1, 100, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Name/Name
            {58, 9, 3, 100, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Name/Name
            {58, 9, 4, 4, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Name/Name
            {58, 9, 3, 1, 100, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Name/Name
            {58, 9, 2, 102, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Name/Name
            {73, 6, 100, 2, 2}, // /AppConfig/QueryServiceConfig/S3/DefaultSettings/Value/Value
            {73, 11, 6, 2, 2}, // /AppConfig/QueryServiceConfig/Generic/DefaultSettings/Value/Value
            {73, 15, 101, 100, 3, 2, 1, 1}, // /AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Activation/ByHour/Hour/Hour
            {73, 15, 101, 100, 3, 2, 2, 2}, // /AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Activation/ByHour/Percentage/Percentage
            {73, 15, 101, 100, 1, 1}, // /AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Name/Name
            {73, 15, 101, 100, 2, 2}, // /AppConfig/QueryServiceConfig/Yt/ClusterMapping/Settings/Value/Value
            {73, 15, 102, 3, 2, 1, 1}, // /AppConfig/QueryServiceConfig/Yt/DefaultSettings/Activation/ByHour/Hour/Hour
            {73, 15, 102, 3, 2, 2, 2}, // /AppConfig/QueryServiceConfig/Yt/DefaultSettings/Activation/ByHour/Percentage/Percentage
            {73, 15, 102, 1, 1}, // /AppConfig/QueryServiceConfig/Yt/DefaultSettings/Name/Name
            {73, 15, 102, 2, 2}, // /AppConfig/QueryServiceConfig/Yt/DefaultSettings/Value/Value
            {73, 15, 100, 1, 1}, // /AppConfig/QueryServiceConfig/Yt/RemoteFilePatterns/Pattern/Pattern
            {73, 15, 100, 2, 2}, // /AppConfig/QueryServiceConfig/Yt/RemoteFilePatterns/Cluster/Cluster
            {73, 15, 100, 3, 3}, // /AppConfig/QueryServiceConfig/Yt/RemoteFilePatterns/Path/Path
            {58, 9, 6, 1, 100, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Settings/Value/Value
            {58, 9, 5, 1, 100, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/S3/ClusterMapping/Settings/Value/Value
            {58, 9, 6, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/DefaultSettings/Value/Value
            {58, 9, 4, 4, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/DefaultSettings/Value/Value
            {58, 9, 4, 1, 100, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Ydb/ClusterMapping/Settings/Value/Value
            {73, 6, 1, 100, 2, 2}, // /AppConfig/QueryServiceConfig/S3/ClusterMapping/Settings/Value/Value
            {58, 9, 5, 100, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/S3/DefaultSettings/Value/Value
            {58, 9, 2, 102, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultSettings/Value/Value
            {58, 9, 3, 100, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/DefaultSettings/Value/Value
            {58, 9, 3, 1, 100, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Pq/ClusterMapping/Settings/Value/Value
            {58, 9, 10, 6, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Generic/DefaultSettings/Value/Value
            {58, 9, 9, 1, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/YqlCore/Flags/Name/Name
            {58, 9, 9, 2, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/YqlCore/QPlayerActivation/ByHour/Hour/Hour
            {58, 9, 9, 2, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/YqlCore/QPlayerActivation/ByHour/Percentage/Percentage
            {58, 9, 6, 1, 8, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Path/Project/Project
            {58, 9, 6, 1, 8, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Solomon/ClusterMapping/Path/Cluster/Cluster
            {58, 9, 2, 6, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/WithHiddenByHour/Hour/Hour
            {58, 9, 2, 2, 1, 1}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultAutoByHour/Hour/Hour
            {58, 9, 2, 6, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/WithHiddenByHour/Percentage/Percentage
            {58, 9, 2, 2, 2, 2}, // /AppConfig/FederatedQueryConfig/Gateways/Dq/DefaultAutoByHour/Percentage/Percentage
            {17, 10, 1, 1}, // /AppConfig/KQPConfig/Settings/Name/Name
            {17, 10, 2, 2}, // /AppConfig/KQPConfig/Settings/Value/Value
            {1, 7, 1, 1}, // /AppConfig/ActorSystemConfig/ServiceExecutor/ServiceName/ServiceName
            {1, 7, 2, 2}, // /AppConfig/ActorSystemConfig/ServiceExecutor/ExecutorId/ExecutorId
            {27, 67, 1, 1, 1}, // /AppConfig/SqsConfig/AuthConfig/OauthToken/TokenFile/TokenFile
            {27, 67, 2, 1, 1}, // /AppConfig/SqsConfig/AuthConfig/Jwt/JwtFile/JwtFile
        };

        for (const auto& [file, typeToField] : fields) {
            for (const auto& [type, fieldToPaths] : typeToField) {
                for (const auto& [field, paths] : fieldToPaths) {
                    for (const auto& path : paths) {
                        UNIT_ASSERT_C(allowedPaths.contains(path.FieldPath), Sprintf("Adding new required fields in config is not allowed: %s", path.FieldPath.c_str()));
                        UNIT_ASSERT_C(allowedNumberPaths.contains(path.FieldNumberPath), Sprintf("Adding new required fields in config is not allowed: %s", path.FieldPath.c_str()));
                    }
                }
            }
        }
    }
}
