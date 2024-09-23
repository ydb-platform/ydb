#include "http.h"

#include <ydb/library/protobuf_printer/security_printer.h>

#include <util/random/random.h>
#include <util/string/builder.h>

namespace NKikimr::NConsole::NHttp {

void OutputStyles(IOutputStream &os)
{
    os << "<style>" << Endl
       << ".collapse-ref {" << Endl
       << "  cursor: pointer" << Endl
       << "}" << Endl
       << ".tab-left {" << Endl
       << "  margin-left: 20px" << Endl
       << "}" << Endl
       << ".collapse-ref.empty {" << Endl
       << "  color: red !important" << Endl
       << "}" << Endl
       << ".collapse-ref.dynamic {" << Endl
       << "  color: green !important" << Endl
       << "}" << Endl
       << "</style>" << Endl;
}

void OutputStaticPart(IOutputStream &os)
{
    HTML(os) {
        HEAD() {
            os << "<link rel='stylesheet' href='../static/css/bootstrap.min.css'>" << Endl
               << "<script language='javascript' type='text/javascript' src='../static/js/jquery.min.js'></script>" << Endl
               << "<script language='javascript' type='text/javascript' src='../static/js/bootstrap.min.js'></script>" << Endl;
        }
        OutputStyles(os);
    }
}

void PrintField(IOutputStream &os, const NKikimrConfig::TAppConfig &config, const google::protobuf::FieldDescriptor *field) {
    auto *reflection = config.GetReflection();
    const auto options = field->options();
    HTML(os) {
        PRE() {
            if (options.GetExtension(Ydb::sensitive)) {
                os << "***";
                return;
            }
            switch (field->cpp_type()) {
            case ::google::protobuf::FieldDescriptor::CPPTYPE_INT32:
                os << reflection->GetInt32(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_INT64:
                os << reflection->GetInt64(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_UINT32:
                os << reflection->GetUInt32(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_UINT64:
                os << reflection->GetUInt64(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE:
                os << reflection->GetDouble(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_FLOAT:
                os << reflection->GetFloat(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_BOOL:
                os << reflection->GetBool(config, field);
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_ENUM:
                os << reflection->GetEnum(config, field)->name();
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_STRING:
                if (field->is_repeated()) {
                    int count = reflection->FieldSize(config, field);
                    for (int index = 0; index < count; ++index) {
                        os << "[" << index << "]: "
                            << reflection->GetRepeatedString(config, field, index) << '\n';
                    }
                } else {
                    os << reflection->GetString(config, field);
                }
                break;
            case ::google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
            {
                const auto* desc = field->message_type();
                TSecurityTextFormatPrinterBase printer(desc);
                if (field->is_repeated()) {
                    int count = reflection->FieldSize(config, field);
                    for (int index = 0; index < count; ++index) {
                        TString str;
                        printer.PrintToString(reflection->GetRepeatedMessage(config, field, index), &str);
                        os << "[" << index << "]:\n" << str;
                    }
                } else {
                    TString str;
                    printer.PrintToString(reflection->GetMessage(config, field), &str);
                    os << str;
                }
                break;
            }
            default:
                os << "&lt;unsupported value type&gt;";
            }
        }
    }
};

void OutputConfigHTML(IOutputStream &os, const NKikimrConfig::TAppConfig &config)
{
    HTML(os) {
        DIV_CLASS("tab-left") {
            auto *reflection = config.GetReflection();
            std::vector<const ::google::protobuf::FieldDescriptor *> fields;
            reflection->ListFields(config, &fields);

            for (auto field : fields) {
                TString id = TStringBuilder() << field->name() << "-" << RandomNumber<ui64>();
                COLLAPSED_REF_CONTENT(id, field->name()) {
                    DIV_CLASS("tab-left") {
                        PrintField(os, config, field);
                    }
                }
                os << "<br/>" << Endl;
            }
        }
    }
}

void OutputConfigDebugInfoHTML(
    IOutputStream &os,
    const NKikimrConfig::TAppConfig &initialConfig,
    const NKikimrConfig::TAppConfig &yamlConfig,
    const NKikimrConfig::TAppConfig &protoConfig,
    const THashMap<ui32, TConfigItemInfo>& configInitInfo,
    const THashSet<ui32> &dynamicKinds,
    const THashSet<ui32> &nonYamlKinds,
    bool yamlEnabled)
{
    HTML(os) {
        DIV_CLASS("tab-left") {
            auto *desc = initialConfig.GetDescriptor();
            auto *reflection = initialConfig.GetReflection();

            for (int i = 0; i < desc->field_count(); i++) {
                auto *field = desc->field(i);
                auto tag = field->number();

                bool yaml = false;
                bool empty = false;
                bool fallback = false;

                const NKikimrConfig::TAppConfig *config = nullptr;

                auto chooseSource = [&](auto& primaryConfig, auto& fallbackConfig) {
                    if (field->is_repeated()) {
                        if (reflection->FieldSize(primaryConfig, field)) {
                            config = &primaryConfig;
                        } else if (reflection->FieldSize(fallbackConfig, field)) {
                            config = &fallbackConfig;
                            fallback = true;
                        } else {
                            empty = true;
                        }
                    } else {
                        if (reflection->HasField(primaryConfig, field)) {
                            config = &primaryConfig;
                        } else if (reflection->HasField(fallbackConfig, field)) {
                            config = &fallbackConfig;
                            fallback = true;
                        } else {
                            empty = true;
                        }
                    }
                };

                if (dynamicKinds.contains(tag)) {
                    if (yamlEnabled && !nonYamlKinds.contains(tag)) {
                        chooseSource(yamlConfig, initialConfig);
                        yaml = true;
                    } else {
                        chooseSource(protoConfig, initialConfig);
                    }
                } else {
                    fallback = true;
                    if (field->is_repeated()) {
                        if (reflection->FieldSize(initialConfig, field)) {
                            config = &initialConfig;
                        } else {
                            empty = true;
                        }
                    } else {
                        if (reflection->HasField(initialConfig, field)) {
                            config = &initialConfig;
                        } else {
                            empty = true;
                        }
                    }
                }

                TString id = TStringBuilder() << field->name() << "-" << RandomNumber<ui64>();
                COLLAPSED_REF_CONTENT(id, field->name(), (empty ? "empty" : ""), (yaml ? "yaml" : ""), (fallback || empty ? "" : "dynamic")) {
                    DIV_CLASS("tab-left") {
                        if (empty) {
                            os << "<div class=\"alert alert-primary\" role=\"alert\">" << Endl;
                            os << "This config item isn't set" << Endl;
                            os << "</div>" << Endl;
                        } else {
                            if (!configInitInfo.empty()) {
                                os << "<div class=\"alert alert-primary\" role=\"alert\"><ol type=\"1\">" << Endl;
                                if (auto it = configInitInfo.find(tag); it != configInitInfo.end()) {
                                    for (const auto& update : it->second.Updates) {
                                        os << "<li><b>" << update.Kind << "</b> at <b>" << update.File << "</b>:" << update.Line << "<br/></li>";
                                    }
                                }
                                if (yaml) {
                                    os << "<li>Overwritten by dynamic yaml config</li>";
                                } else if (!fallback) {
                                    os << "<li>Overwritten by dynamic proto config</li>";
                                }
                                os << "</ol></div>" << Endl;
                            } else {
                                os << "<div class=\"alert alert-primary\" role=\"alert\">" << Endl;
                                if (fallback) {
                                    os << "This config item is set in static config";
                                } else if (yaml) {
                                    os << "This config item is set in dynamic yaml config";
                                } else {
                                    os << "This config item is set in dynamic proto config";
                                }
                                os << "</div>" << Endl;
                            }
                            PrintField(os, *config, field);
                        }
                    }
                }
                os << "<br/>" << Endl;
            }
        }
    }
}

void OutputRichConfigHTML(
    IOutputStream &os,
    const NKikimrConfig::TAppConfig &initialConfig,
    const NKikimrConfig::TAppConfig &yamlConfig,
    const NKikimrConfig::TAppConfig &protoConfig,
    const THashSet<ui32> &dynamicKinds,
    const THashSet<ui32> &nonYamlKinds,
    bool yamlEnabled)
{
    OutputConfigDebugInfoHTML(os, initialConfig, yamlConfig, protoConfig, {}, dynamicKinds, nonYamlKinds, yamlEnabled);
}

} // namespace NKikimr::NConsole::NHttp
