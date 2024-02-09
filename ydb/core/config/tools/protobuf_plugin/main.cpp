#include <map>

#include <util/system/compiler.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/subst.h>

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <ydb/core/config/protos/marker.pb.h>

#include <ydb/public/lib/protobuf/base_message_generator.h>
#include <ydb/public/lib/protobuf/helpers.h>
#include <ydb/public/lib/protobuf/macro.h>

using namespace google::protobuf::compiler;
using namespace google::protobuf;
using namespace NKikimr::NProtobuf;

constexpr TStringBuf PLUGIN_NAME = "config";

class TMessageGenerator
    : public TBaseMessageGenerator
{
private:

    void GenerateConfigRoot(TVars vars) {
        for (auto i = 0; i < Message->field_count(); ++i) {
            const FieldDescriptor* field = Message->field(i);
            if (field->is_repeated()) {
                continue;
            }
            if (auto* fieldMessage = field->message_type()) {
                vars["field"] = field->name();
                vars["fqFieldClass"] = FullyQualifiedClassName(fieldMessage);
                vars["fieldNumber"] = std::to_string(field->number());

                WITH_PLUGIN_MARKUP(Header, PLUGIN_NAME) {
                    Header->Print(vars, "struct T$field$FieldTag {};\n");

                    Header->Print(vars, "constexpr inline static std::tuple<\n");
                    WITH_INDENT(Header) {
                        Header->Print(vars,
                            "bool ($fqMessageClass$::*)() const,\n"
                            "const $fqFieldClass$& ($fqMessageClass$::*)() const,\n"
                            "$fqFieldClass$* ($fqMessageClass$::*)()\n"
                        );
                    }
                    Header->Print(vars, "> GetFieldAccessorsByFieldTag($fqMessageClass$::T$field$FieldTag) {\n");
                    WITH_INDENT(Header) {
                        Header->Print(vars, "return std::tuple{\n");
                        WITH_INDENT(Header) {
                            Header->Print(vars,
                                "&$fqMessageClass$::Has$field$,\n"
                                "&$fqMessageClass$::Get$field$,\n"
                                "&$fqMessageClass$::Mutable$field$\n"
                            );
                        }
                        Header->Print(vars, "};\n");
                    }
                    Header->Print(vars, "}\n");

                    Header->Print(vars, "constexpr inline static ::NProtoBuf::uint32 GetFieldIdByFieldTag($fqMessageClass$::T$field$FieldTag) {\n");
                    WITH_INDENT(Header) {
                        Header->Print(vars, "return $fieldNumber$;\n");
                    }
                    Header->Print(vars, "}\n");
                }
            }
        }
    }

    void GenerateCombinedType(TVars vars) {
        TMap<TString, TSet<const FieldDescriptor*>> outputs;
        for (auto i = 0; i < Message->field_count(); ++i) {
            const FieldDescriptor* field = Message->field(i);
            auto opts = field->options();
            for (int i = 0; i < opts.ExtensionSize(NKikimrConfig::NMarkers::CopyTo); ++i) {
                outputs[opts.GetExtension(NKikimrConfig::NMarkers::CopyTo, i)].insert(field);
            }
        }

        for (const auto& [output, fields] : outputs) {
            vars["output"] = output;
            WITH_PLUGIN_MARKUP(Header, PLUGIN_NAME) {
                Header->Print(vars, "template <class TOut>\n");
                Header->Print(vars, "void CopyTo$output$(TOut& out) const {\n");
                Header->Indent();
                for (const auto* field : fields) {
                    vars["field"] = field->name();

                    if (!field->is_repeated()) {
                        if (field->message_type()) {
                            Header->Print(vars, "if (Has$field$()) {\n");
                            WITH_INDENT(Header) {
                                Header->Print(vars, "out.Mutable$field$()->CopyFrom(Get$field$());\n");
                            }
                            Header->Print(vars, "}\n");
                        } else if (field->is_optional()) {
                            Header->Print(vars, "if (Has$field$()) {\n");
                            WITH_INDENT(Header) {
                                Header->Print(vars, "out.Set$field$(Get$field$());\n");
                            }
                            Header->Print(vars, "}\n");
                        } else {
                            Header->Print(vars, "out.Set$field$(Get$field$());\n");
                        }
                    } else {
                        if (field->message_type()) {
                            Header->Print(vars, "for (size_t i = 0; i < $field$Size(); ++i) {\n");
                            WITH_INDENT(Header) {
                                Header->Print(vars, "out.Add$field$()->CopyFrom(Get$field$(i));\n");
                            }
                            Header->Print(vars, "}\n");
                        } else {
                            Header->Print(vars, "for (const auto& field : Get$field$()) {\n");
                            WITH_INDENT(Header) {
                                Header->Print(vars, "out.Add$field$(field);\n");
                            }
                            Header->Print(vars, "}\n");
                        }
                    }
                }
                Header->Outdent();
                Header->Print(vars, "}\n");
            }
        }
    }

    void GenerateSizeFields(TVars vars, const TSet<const FieldDescriptor*>& fields) {
        Header->Print(vars, "size_t $output$Size(const TProtoStringType& str) const {\n");
        WITH_INDENT(Header) {
            Header->Print(vars, "static std::map<TProtoStringType, size_t ($fqMessageClass$::*)() const> sizeHandlers{\n");
            WITH_INDENT(Header) {
                for (const auto* field : fields) {
                    vars["field"] = field->name();
                    Header->Print(vars, "{\"$field$\", &$fqMessageClass$::$field$Size},\n");
                }
            }
            Header->Print(vars, "};\n");

            Header->Print(vars, "auto it = sizeHandlers.find(str);\n");
            Header->Print(vars, "return it == sizeHandlers.end() ? 0 : (this->*(it->second))();\n");
        }
        Header->Print(vars, "}\n");
    }

    void GenerateAddFields(TVars vars, const TSet<const FieldDescriptor*>& fields) {
        Header->Print(vars, "$fqFieldClass$* Add$output$(const TProtoStringType& str) {\n");
        WITH_INDENT(Header) {
            Header->Print(vars, "static std::map<TProtoStringType, $fqFieldClass$* ($fqMessageClass$::*)()> addHandlers{\n");
            WITH_INDENT(Header) {
                for (const auto* field : fields) {
                    vars["field"] = field->name();
                    Header->Print(vars, "{\"$field$\", &$fqMessageClass$::Add$field$},\n");
                }
            }
            Header->Print(vars, "};\n");

            Header->Print(vars, "return (this->*addHandlers.at(str))();\n");
        }
        Header->Print(vars, "}\n");
    }

    void GenerateGetFields(TVars vars, const TSet<const FieldDescriptor*>& fields) {
        Header->Print(vars, "const ::google::protobuf::RepeatedPtrField<$fqFieldClass$>& Get$output$(const TProtoStringType& str) const {\n");
        WITH_INDENT(Header) {
            Header->Print(vars, "static std::map<TProtoStringType, const ::google::protobuf::RepeatedPtrField<$fqFieldClass$>& ($fqMessageClass$::*)() const> getHandlers{\n");
            WITH_INDENT(Header) {
                for (const auto* field : fields) {
                    vars["field"] = field->name();
                    Header->Print(vars, "{\"$field$\", &$fqMessageClass$::Get$field$},\n");
                }
            }
            Header->Print(vars, "};\n");

            Header->Print(vars, "return (this->*getHandlers.at(str))();\n");
        }
        Header->Print(vars, "}\n");
    }

    void GenerateMutableFields(TVars vars, const TSet<const FieldDescriptor*>& fields) {
        Header->Print(vars, "::google::protobuf::RepeatedPtrField<$fqFieldClass$>* Mutable$output$(const TProtoStringType& str) {\n");
        WITH_INDENT(Header) {
            Header->Print(vars, "static std::map<TProtoStringType, ::google::protobuf::RepeatedPtrField<$fqFieldClass$>* ($fqMessageClass$::*)()> mutableHandlers{\n");
            WITH_INDENT(Header) {
                for (const auto* field : fields) {
                    vars["field"] = field->name();
                    Header->Print(vars, "{\"$field$\", &$fqMessageClass$::Mutable$field$},\n");
                }
            }
            Header->Print(vars, "};\n");

            Header->Print(vars, "return (this->*mutableHandlers.at(str))();\n");
        }
        Header->Print(vars, "}\n");
    }

    void GenerateFieldsMap(TVars vars) {
        TMap<TString, TSet<const FieldDescriptor*>> outputs;
        for (auto i = 0; i < Message->field_count(); ++i) {
            const FieldDescriptor* field = Message->field(i);
            auto opts = field->options();
            for (int i = 0; i < opts.ExtensionSize(NKikimrConfig::NMarkers::AsMap); ++i) {
                outputs[opts.GetExtension(NKikimrConfig::NMarkers::AsMap, i)].insert(field);
            }
        }

        if (!outputs) {
            return;
        }

        WITH_PLUGIN_MARKUP(HeaderIncludes, PLUGIN_NAME) {
            HeaderIncludes->Print(Vars, "#include <map>\n");
        }

        for (const auto& [output, fields] : outputs) {
            if (auto* fieldMessage = (*fields.begin())->message_type()) { // TODO implement for other classes
                for (const auto* field : fields) {
                    if (fieldMessage->full_name() != field->message_type()->full_name()) {
                        Cerr << "Messages in map MUST have the same type: "
                             << fieldMessage->full_name().c_str() << " != "
                             << field->message_type()->full_name().c_str() << Endl;
                        Y_ABORT("Invariant failed");
                    }

                    Y_ABORT_UNLESS(field->is_repeated(), "Only repeated fields are supported");
                }
                vars["fqFieldClass"] = FullyQualifiedClassName(fieldMessage);
            } else {
                Y_ABORT("Types other than Message is not supported yet.");
            }

            vars["output"] = output;

            WITH_PLUGIN_MARKUP(Header, PLUGIN_NAME) {
                GenerateSizeFields(vars, fields);
                GenerateAddFields(vars, fields);
                GenerateGetFields(vars, fields);
                GenerateMutableFields(vars, fields);
            }
        }
    }

public:

    using TBaseMessageGenerator::TBaseMessageGenerator;

    void Generate() {
        if (Message->options().GetExtension(NKikimrConfig::NMarkers::Root)) {
            GenerateConfigRoot(Vars);
        }

        if (Message->options().GetExtension(NKikimrConfig::NMarkers::CombinedType)) {
            GenerateCombinedType(Vars);
        }

        if (Message->options().GetExtension(NKikimrConfig::NMarkers::WithMapType)) {
            GenerateFieldsMap(Vars);
        }
    }
};

class TCodeGenerator: public CodeGenerator {
    bool Generate(
            const FileDescriptor* file,
            const TProtoStringType&,
            OutputDirectory* output,
            TProtoStringType*) const override final
    {

        for (auto i = 0; i < file->message_type_count(); ++i) {
            TMessageGenerator mg(file->message_type(i), output);
            mg.Generate();
        }

        return true;
    }

    uint64_t GetSupportedFeatures() const override
    {
        return FEATURE_PROTO3_OPTIONAL;
    }

}; // TCodeGenerator

int main(int argc, char* argv[]) {
    TCodeGenerator generator;
    return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
