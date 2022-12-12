#include "http.h"

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
       << "</style>" << Endl;
}

void OutputStaticPart(IOutputStream &os)
{
    HTML(os) {
        HEAD() {
            os << "<link rel='stylesheet' href='https://yastatic.net/bootstrap/3.3.1/css/bootstrap.min.css'>" << Endl
               << "<script language='javascript' type='text/javascript' src='https://yastatic.net/jquery/2.1.3/jquery.min.js'></script>" << Endl
               << "<script language='javascript' type='text/javascript' src='https://yastatic.net/bootstrap/3.3.1/js/bootstrap.min.js'></script>" << Endl;
        }
        OutputStyles(os);
    }
}

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
                        PRE() {
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
                                os << reflection->GetString(config, field);
                                break;
                            case ::google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE:
                                os << reflection->GetMessage(config, field).DebugString();
                                break;
                            default:
                                os << "<unsupported value type>";
                            }
                        }
                    }
                }
                os << "<br/>" << Endl;
            }
        }
    }
}

} // namespace NKikimr::NConsole::NHttp
