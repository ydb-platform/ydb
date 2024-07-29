#include <map>
#include <unordered_set>

#include <util/generic/deque.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/string/subst.h>
#include <util/string/join.h>
#include <util/system/compiler.h>

#include <library/cpp/protobuf/json/util.h>

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <ydb/core/config/protos/marker.pb.h>
#include <ydb/core/config/utils/config_traverse.h>

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
    TString FieldName(const FieldDescriptor* field) const {
        TString name = field->name();
        NProtobufJson::ToSnakeCaseDense(&name);
        return name;
    }

    TString ConstructFullFieldPath(const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field) const {

        TVector<TString> path;
        path.push_back("");
        for (size_t i = 1; i < fieldPath.size(); ++i) {
            TString fieldName = FieldName(fieldPath[i]);
            path.push_back(fieldName);
        }

        return JoinSeq("/", path);
    }

    void GenerateConfigRoot(TVars vars) {
        std::unordered_set<TString> reservedPaths;
        NKikimr::NConfig::Traverse([&](const Descriptor* d, const TDeque<const Descriptor*>& typePath, const TDeque<const FieldDescriptor*>& fieldPath, const FieldDescriptor* field, ssize_t loop) {
            Y_UNUSED(fieldPath, typePath, fieldPath, field, loop);
            if (field && d) {
                for (int i = 0; i < d->reserved_name_count(); ++i) {
                    TString name = d->reserved_name(i);
                    NProtobufJson::ToSnakeCaseDense(&name);
                    reservedPaths.insert(ConstructFullFieldPath(fieldPath, field) + "/" + name);
                }
            }
        }, Message);

        for (int i = 0; i < Message->reserved_name_count(); ++i) {
            TString name = Message->reserved_name(i);
            NProtobufJson::ToSnakeCaseDense(&name);
            reservedPaths.insert(TString("/") + name);
        }

        WITH_PLUGIN_MARKUP(HeaderIncludes, PLUGIN_NAME) {
            HeaderIncludes->Print(Vars, "#include <unordered_set>\n");
        }

        WITH_PLUGIN_MARKUP(Header, PLUGIN_NAME) {
            Header->Print(vars, "inline static const std::unordered_set<TString>& GetReservedChildrenPaths() {\n");
            WITH_INDENT(Header) {
                Header->Print(vars, "static const std::unordered_set<TString> reserved = {\n");
                WITH_INDENT(Header) {
                    for (const auto& path : reservedPaths) {
                        vars["reservedPath"] = path;
                        Header->Print(vars, "\"$reservedPath$\",\n");
                    }
                }
                Header->Print(vars, "};\n");
                Header->Print(vars, "return reserved;\n");
            }
            Header->Print(vars, "}\n");
        }

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

    struct TCopyTo {
        bool Transform = false;
        bool Keep = false;
        TString Recurse = "";
        TString TargetFieldName;
    };

    void GenerateCopyToSingularField(const TVars& vars, const FieldDescriptor* const field, const TCopyTo& copyTo) {
        if (copyTo.Transform) {
            Header->Print(vars, "if (Has$field$()) {\n");
            Header->Print(vars, "const auto& field = Get$field$();\n");
            Header->Print(vars,
                          "out.Set$target$("
                              "Transform$field$To$target$For$output$<"
                                  "typename std::remove_reference<decltype(Get$field$())>::type, "
                                  "typename std::remove_cvref<decltype(out.Get$target$())>::type>("
                                      "&field)"
                          ");\n");
            Header->Print(vars, "} else {\n");
            Header->Print(vars,
                          "out.Set$target$("
                              "Transform$field$To$target$For$output$<"
                                  "typename std::remove_reference<decltype(Get$field$())>::type, "
                                  "typename std::remove_cvref<decltype(out.Get$target$())>::type>("
                                      "nullptr)"
                          ");\n");
            Header->Print(vars, "}\n");
        } else if (field->message_type()) {
            if (!copyTo.Keep) {
                Header->Print(vars, "if (Has$field$()) {\n");
            } else {
                Header->Print(vars, "if (Has$field$() && !out.Has$target$()) {\n");
            }
            WITH_INDENT(Header) {
                if (!copyTo.Recurse) {
                    // this static_assert is critically important
                    // without it user will get error only at runtime!
                    Header->Print(vars, "static_assert(std::is_same_v<typename std::remove_cvref_t<decltype(out.Get$target$())>, "
                                                                     "typename std::remove_cvref_t<decltype(this->Get$field$())>>, \"FieldType mismatch\");\n");
                    Header->Print(vars, "out.Mutable$target$()->CopyFrom(Get$field$());\n");
                } else {
                    Header->Print(vars, "auto& target = *out.Mutable$target$();\n");
                    Header->Print(vars, "Get$field$().CopyTo$recurse$(target);\n");
                }
            }
            Header->Print(vars, "}\n");
        } else if (field->is_optional()) {
            if (!copyTo.Keep) {
                Header->Print(vars, "// if you get error below it means that target field is not optional and keep option isn't applicable\n");
                Header->Print(vars, "if (Has$field$()) {\n");
            } else {
                Header->Print(vars, "if (Has$field$() && !out.Has$target$()) {\n");
            }
            WITH_INDENT(Header) {
                Header->Print(vars, "out.Set$target$(Get$field$());\n");
            }
            Header->Print(vars, "}\n");
        } else {
            Y_ABORT_IF(copyTo.Keep, "Can't keep non optional and non message field");
            Header->Print(vars, "out.Set$target$(Get$field$());\n");
        }
    }

    void GenerateCopyToRepeatedField(const TVars& vars, const FieldDescriptor* const field, const TCopyTo& copyTo) {
        if (copyTo.Transform) {
            Header->Print(vars, "for (const auto& field : Get$field$()) {\n");
            WITH_INDENT(Header) {
                Header->Print(vars,
                              "out.Add$target$("
                                  "Transform$field$To$target$For$output$<"
                                      "typename std::remove_reference<decltype(Get$field$(0))>::type, "
                                      "typename std::remove_cvref<decltype(out.Get$target$(0))>::type>("
                                          "&field)"
                              ");\n");
            }
            Header->Print(vars, "}\n");
            return;
        }
        if (copyTo.Keep) {
            Header->Print(vars, "if (!out.$target$Size()) {\n");
            Header->Indent();
        }
        if (field->message_type()) {
            Header->Print(vars, "for (size_t i = 0; i < $field$Size(); ++i) {\n");
            WITH_INDENT(Header) {
                if (!copyTo.Recurse) {
                    // this static_assert is critically important
                    // without it user will get error only at runtime!
                    Header->Print(vars, "static_assert(std::is_same_v<typename std::remove_cvref_t<decltype(*out.Add$target$())>, "
                                                                     "typename std::remove_cvref_t<decltype(this->Get$field$(0))>>, \"FieldType mismatch\");\n");
                    Header->Print(vars, "out.Add$target$()->CopyFrom(Get$field$(i));\n");
                } else {
                    Header->Print(vars, "auto& target = *out.Add$field$();\n");
                    Header->Print(vars, "Get$field$(i).CopyTo$recurse$(target);\n");
                }
            }
            Header->Print(vars, "}\n");
        } else {
            Header->Print(vars, "for (const auto& field : Get$field$()) {\n");
            WITH_INDENT(Header) {
                Header->Print(vars, "// if you get error below it means that target type either not scalar or not repeated\n");
                Header->Print(vars, "out.Add$target$(field);\n");
            }
            Header->Print(vars, "}\n");
        }
        if (copyTo.Keep) {
            Header->Outdent();
            Header->Print(vars, "}\n");
        }
    }

    void GenerateCombinedType(TVars vars) {
        TMap<TString, TMap<std::pair<const FieldDescriptor*, TString>, TCopyTo>> outputs;
        for (auto i = 0; i < Message->field_count(); ++i) {
            const FieldDescriptor* field = Message->field(i);
            auto opts = field->options();
            for (int i = 0; i < opts.ExtensionSize(NKikimrConfig::NMarkers::CopyTo); ++i) {
                Y_ABORT_IF(!opts.GetExtension(NKikimrConfig::NMarkers::CopyTo, i), "Target MUST be non-empty");
                outputs[opts.GetExtension(NKikimrConfig::NMarkers::CopyTo, i)][{field, field->name()}] = TCopyTo{
                        .Transform = false,
                        .Keep = false,
                        .Recurse = "",
                        .TargetFieldName = field->name(),
                    };
            }

            for (int i = 0; i < opts.ExtensionSize(NKikimrConfig::NMarkers::AdvancedCopyTo); ++i) {
                auto option = opts.GetExtension(NKikimrConfig::NMarkers::AdvancedCopyTo, i);
                Y_ABORT_IF(!option.GetTarget(), "Target MUST be non-empty");
                outputs[option.GetTarget()][{field, option.GetRename()}] = TCopyTo{
                        .Transform = option.GetTransform(),
                        .Keep = option.GetKeep(),
                        .Recurse = option.GetRecurse(),
                        .TargetFieldName = option.HasRename() ? option.GetRename() : field->name(),
                    };
            }
        }

        for (const auto& [output, fields] : outputs) {
            vars["output"] = output;
            WITH_PLUGIN_MARKUP(Header, PLUGIN_NAME) {
                for (const auto& [fieldWithRename, copyTo] : fields) {
                    const auto* field = fieldWithRename.first;
                    vars["field"] = field->name();
                    vars["target"] = copyTo.TargetFieldName;
                    if (copyTo.Transform) {
                        Header->Print(vars, "/* right instance **must** be defined in user code */\n");
                        Header->Print(vars, "template <class TIn, class TOut>\n");
                        Header->Print(vars, "static TOut Transform$field$To$target$For$output$(const TIn* from);\n");
                    }
                }
                Header->Print(vars, "template <class TOut>\n");
                Header->Print(vars, "void CopyTo$output$(TOut& out) const {\n");
                WITH_INDENT(Header) {
                    for (const auto& [fieldWithRename, copyTo] : fields) {
                        const auto* field = fieldWithRename.first;
                        vars["field"] = field->name();
                        vars["target"] = copyTo.TargetFieldName;
                        vars["recurse"] = copyTo.Recurse;

                        Y_ABORT_IF(copyTo.Transform && field->message_type(), "Transform currently does not support Message type fields");
                        Y_ABORT_IF(copyTo.Transform && copyTo.Keep, "Transform currently does not Keep flag");
                        Y_ABORT_IF(copyTo.Transform && copyTo.Recurse, "Transform currently does not Recurse flag");

                        if (!field->is_repeated()) {
                            GenerateCopyToSingularField(vars, field, copyTo);
                        } else {
                            GenerateCopyToRepeatedField(vars, field, copyTo);
                        }
                    }
                }
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
