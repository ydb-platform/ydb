#include "descriptor.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/protobuf/dynamic_prototype/dynamic_prototype.h>
#include <library/cpp/protobuf/dynamic_prototype/generate_file_descriptor_set.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <util/generic/hash.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/stream/mem.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/string/cast.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace NProtoBuf;

static TString SerializeFileDescriptorSet(const FileDescriptorSet& proto) {
    const auto size = proto.ByteSize();
    TTempBuf data(size);
    proto.SerializeWithCachedSizesToArray((ui8*)data.Data());

    TStringStream str;
    {
        TZLibCompress comp(&str, ZLib::GZip);
        comp.Write(data.Data(), size);
    }
    return str.Str();
}

static bool ParseFileDescriptorSet(const TStringBuf& data, FileDescriptorSet* proto) {
    TMemoryInput input(data.data(), data.size());
    TString buf = TZLibDecompress(&input).ReadAll();

    if (!proto->ParseFromArray(buf.data(), buf.size())) {
        return false;
    }
    return true;
}

TDynamicInfo::TDynamicInfo(TDynamicPrototypePtr dynamicPrototype)
    : DynamicPrototype(dynamicPrototype)
    , SkipBytes_(0)
{
}

TDynamicInfo::~TDynamicInfo() {
}

TDynamicInfoRef TDynamicInfo::Create(const TStringBuf& typeConfig) {
    auto data = ParseTypeConfig(typeConfig);
    const TString& meta = Base64Decode(data.Metadata);
    const TString& name = data.MessageName;
    FileDescriptorSet set;

    if (!ParseFileDescriptorSet(meta, &set)) {
        ythrow yexception() << "can't parse metadata";
    }

    auto info = MakeIntrusive<TDynamicInfo>(TDynamicPrototype::Create(set, name, true));

    info->EnumFormat_ = data.EnumFormat;
    info->ProtoFormat_ = data.ProtoFormat;
    info->Recursion_ = data.Recursion;
    info->YtMode_ = data.YtMode;
    info->SkipBytes_ = data.SkipBytes;
    info->OptionalLists_ = data.OptionalLists;
    info->SyntaxAware_ = data.SyntaxAware;
    return info;
}

const Descriptor* TDynamicInfo::Descriptor() const {
    return DynamicPrototype->GetDescriptor();
}

EEnumFormat TDynamicInfo::GetEnumFormat() const {
    return EnumFormat_;
}

ERecursionTraits TDynamicInfo::GetRecursionTraits() const {
    return Recursion_;
}

bool TDynamicInfo::GetYtMode() const {
    return YtMode_;
}

bool TDynamicInfo::GetOptionalLists() const {
    return OptionalLists_;
}

bool TDynamicInfo::GetSyntaxAware() const {
    return SyntaxAware_;
}

TAutoPtr<Message> TDynamicInfo::MakeProto() {
    return DynamicPrototype->CreateUnsafe();
}

TAutoPtr<Message> TDynamicInfo::Parse(const TStringBuf& data) {
    auto mut = MakeProto();
    TStringBuf tmp(data);

    if (SkipBytes_) {
        tmp = TStringBuf(tmp.data() + SkipBytes_, tmp.size() - SkipBytes_);
    }

    switch (ProtoFormat_) {
        case PF_PROTOBIN: {
            if (!mut->ParseFromArray(tmp.data(), tmp.size())) {
                ythrow yexception() << "can't parse protobin message";
            }
            break;
        }
        case PF_PROTOTEXT: {
            io::ArrayInputStream si(tmp.data(), tmp.size());
            if (!TextFormat::Parse(&si, mut.Get())) {
                ythrow yexception() << "can't parse prototext message";
            }
            break;
        }
        case PF_JSON: {
            NJson::TJsonValue value;

            if (NJson::ReadJsonFastTree(tmp, &value)) {
                NProtobufJson::Json2Proto(value, *mut);
            } else {
                ythrow yexception() << "can't parse json value";
            }
            break;
        }
    }

    return mut;
}

TString TDynamicInfo::Serialize(const Message& proto) {
    TString result;
    switch (ProtoFormat_) {
        case PF_PROTOBIN: {
            result.ReserveAndResize(proto.ByteSize());
            if (!proto.SerializeToArray(result.begin(), result.size())) {
                ythrow yexception() << "can't serialize protobin message";
            }
            break;
        }
        case PF_PROTOTEXT: {
            if (!TextFormat::PrintToString(proto, &result)) {
                ythrow yexception() << "can't serialize prototext message";
            }
            break;
        }
        case PF_JSON: {
            NJson::TJsonValue value;
            NProtobufJson::Proto2Json(proto, value);
            result = NJson::WriteJson(value);
            break;
        }
    }
    return result;
}

TString GenerateProtobufTypeConfig(
    const Descriptor* descriptor,
    const TProtoTypeConfigOptions& options) {
    NJson::TJsonValue ret(NJson::JSON_MAP);

    ret["name"] = descriptor->full_name();
    ret["meta"] = Base64Encode(
        SerializeFileDescriptorSet(GenerateFileDescriptorSet(descriptor)));

    if (options.SkipBytes > 0) {
        ret["skip"] = options.SkipBytes;
    }

    switch (options.ProtoFormat) {
        case PF_PROTOBIN:
            break;
        case PF_PROTOTEXT:
            ret["format"] = "prototext";
            break;
        case PF_JSON:
            ret["format"] = "json";
            break;
    }

    if (!options.OptionalLists) {
        ret["lists"]["optional"] = false;
    }

    if (options.SyntaxAware) {
        ret["syntax"]["aware"] = options.SyntaxAware;
    }

    switch (options.EnumFormat) {
        case EEnumFormat::Number:
            break;
        case EEnumFormat::Name:
            ret["view"]["enum"] = "name";
            break;
        case EEnumFormat::FullName:
            ret["view"]["enum"] = "full_name";
            break;
    }

    switch (options.Recursion) {
        case ERecursionTraits::Fail:
            break;
        case ERecursionTraits::Ignore:
            ret["view"]["recursion"] = "ignore";
            break;
        case ERecursionTraits::Bytes:
            ret["view"]["recursion"] = "bytes";
            break;
    }

    if (options.YtMode) {
         ret["view"]["yt_mode"] = true;
    }

    return NJson::WriteJson(ret, false);
}

TProtoTypeConfig ParseTypeConfig(const TStringBuf& config) {
    if (config.empty()) {
        ythrow yexception() << "empty metadata";
    }

    switch (config[0]) {
        case '#': {
            auto plus = config.find('+');

            if (config[0] != '#') {
                ythrow yexception() << "unknown version of metadata format";
            }
            if (plus == TStringBuf::npos) {
                ythrow yexception() << "invalid metadata";
            }

            TProtoTypeConfig result;

            result.MessageName = TStringBuf(config.begin() + 1, plus - 1);
            result.Metadata = TStringBuf(config.begin() + 1 + plus, config.size() - plus - 1);
            result.SkipBytes = 0;

            return result;
        }

        case '{': {
            NJson::TJsonValue value;

            if (NJson::ReadJsonFastTree(config, &value)) {
                TProtoTypeConfig result;
                TString protoFormat = value["format"].GetStringSafe("protobin");
                TString enumFormat = value["view"]["enum"].GetStringSafe("number");
                TString recursion = value["view"]["recursion"].GetStringSafe("fail");

                result.MessageName = value["name"].GetString();
                result.Metadata = value["meta"].GetString();
                result.SkipBytes = value["skip"].GetIntegerSafe(0);
                result.OptionalLists = value["lists"]["optional"].GetBooleanSafe(true);
                result.SyntaxAware = value["syntax"]["aware"].GetBooleanSafe(false);
                result.YtMode = value["view"]["yt_mode"].GetBooleanSafe(false);

                if (protoFormat == "protobin") {
                    result.ProtoFormat = PF_PROTOBIN;
                } else if (protoFormat == "prototext") {
                    result.ProtoFormat = PF_PROTOTEXT;
                } else if (protoFormat == "json") {
                    result.ProtoFormat = PF_JSON;
                } else {
                    ythrow yexception() << "unsupported format " << protoFormat;
                }

                if (enumFormat == "number") {
                    result.EnumFormat = EEnumFormat::Number;
                } else if (enumFormat == "name") {
                    result.EnumFormat = EEnumFormat::Name;
                } else if (enumFormat == "full_name") {
                    result.EnumFormat = EEnumFormat::FullName;
                } else {
                    ythrow yexception() << "unsupported enum representation "
                                        << enumFormat;
                }

                if (recursion == "fail") {
                    result.Recursion = ERecursionTraits::Fail;
                } else if (recursion == "ignore") {
                    result.Recursion = ERecursionTraits::Ignore;
                } else if (recursion == "bytes") {
                    result.Recursion = ERecursionTraits::Bytes;
                } else {
                    ythrow yexception() << "unsupported recursion trait "
                                        << recursion;
                }

                return result;
            } else {
                ythrow yexception() << "can't parse json metadata";
            }
        }

        default:
            ythrow yexception() << "invalid control char "
                                << TStringBuf(config.data(), 1);
    }
}
