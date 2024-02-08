#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/api/protos/annotations/validation.pb.h>
#include <ydb/public/lib/protobuf/helpers.h>
#include <ydb/public/lib/protobuf/scoped_file_printer.h>

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/subst.h>

namespace NKikimr::NValidation {

using namespace google::protobuf::compiler;
using namespace google::protobuf;
using namespace NKikimr::NProtobuf;

using TVariables = std::map<TString, TString>;
using TPrinter = NKikimr::NProtobuf::TScopedFilePrinter;

bool IsScalarType(const FieldDescriptor* field) {
    switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
        case FieldDescriptor::CPPTYPE_INT64:
        case FieldDescriptor::CPPTYPE_UINT32:
        case FieldDescriptor::CPPTYPE_UINT64:
        case FieldDescriptor::CPPTYPE_DOUBLE:
        case FieldDescriptor::CPPTYPE_FLOAT:
        case FieldDescriptor::CPPTYPE_BOOL:
        case FieldDescriptor::CPPTYPE_STRING:
            return true;
        default:
            return false;
    }
}

class TFieldGenerator: public TThrRefBase {
    void Required(TPrinter& printer) const {
        Y_ABORT_UNLESS(!Field->is_repeated(), "Repeated fields cannot be required or not");

        if (Field->options().GetExtension(Ydb::required)) {
            if (Field->cpp_type() == FieldDescriptor::CPPTYPE_STRING) {
                printer->Print(Vars, "if ($field$().empty()) {\n");
            } else if (Field->has_presence()) {
                printer->Print(Vars, "if (!has_$field$()) {\n");
            } else {
                Y_FAIL_S(" Field type = " << Field->type_name() << " cannot be required or not");
            }

            printer->Indent();
            printer->Print(Vars, "__err = \"$field$ is required but not set\";\n");
            printer->Print(Vars, "return false;\n");
            printer->Outdent();
            printer->Print(Vars, "}\n");
        }
    }

    void CheckLimit(TPrinter& printer, const Ydb::Limit& limit, TVariables vars) const {
        switch (limit.kind_case()) {
        case Ydb::Limit::kRange:
            vars["min"] = ToString(limit.range().min());
            vars["max"] = ToString(limit.range().max());
            printer->Print(vars, "if (!($min$ <= $getter$ && $getter$ <= $max$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not in [$min$; $max$]\";\n");
            break;

        case Ydb::Limit::kLt:
            vars["lt"] = ToString(limit.lt());
            printer->Print(vars, "if (!($getter$ < $lt$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not < $lt$\";\n");
            break;

        case Ydb::Limit::kLe:
            vars["le"] = ToString(limit.le());
            printer->Print(vars, "if (!($getter$ <= $le$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not <= $le$\";\n");
            break;

        case Ydb::Limit::kEq:
            vars["eq"] = ToString(limit.eq());
            printer->Print(vars, "if (!($getter$ == $eq$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not == $eq$\";\n");
            break;

        case Ydb::Limit::kGe:
            vars["ge"] = ToString(limit.ge());
            printer->Print(vars, "if (!($getter$ >= $ge$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not >= $ge$\";\n");
            break;

        case Ydb::Limit::kGt:
            vars["gt"] = ToString(limit.gt());
            printer->Print(vars, "if (!($getter$ > $gt$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not > $gt$\";\n");
            break;

        default:
            Y_ABORT("Unknown limit type");
        }

        printer->Print(vars, "return false;\n");
        printer->Outdent();
        printer->Print(vars, "}\n");
    }

    static TString BuildValueChecker(const TString& getter, TStringBuf annValue) {
        if (annValue.Contains('<') || annValue.Contains('>') || annValue.Contains('=')) {
            return TStringBuilder() << getter << " " << annValue;
        }

        if ((annValue.StartsWith('[') || annValue.StartsWith('('))
            && (annValue.EndsWith(']') || annValue.EndsWith(')'))
            && annValue.Contains(';')) {

            const bool leftInclusive = annValue.StartsWith('[');
            const bool rightInclusive = annValue.EndsWith(']');

            annValue.Skip(1).Chop(1);

            const TStringBuf left = annValue.Before(';');
            const TStringBuf right = annValue.After(';');

            return TStringBuilder()
                << left << " " << (leftInclusive ? "<=" : "<")
                << " " << getter << " && " << getter << " "
                << (rightInclusive ? "<=" : "<") << " " << right;
        }

        Y_FAIL_S("Invalid value: " << annValue);
    }

    void CheckValue(TPrinter& printer, const FieldDescriptor* field, TVariables vars) const {
        switch (field->cpp_type()) {
        case FieldDescriptor::CPPTYPE_INT32:
        case FieldDescriptor::CPPTYPE_INT64:
        case FieldDescriptor::CPPTYPE_UINT32:
        case FieldDescriptor::CPPTYPE_UINT64:
        case FieldDescriptor::CPPTYPE_DOUBLE:
        case FieldDescriptor::CPPTYPE_FLOAT:
        case FieldDescriptor::CPPTYPE_BOOL:
        case FieldDescriptor::CPPTYPE_STRING:
            printer->Print(vars, "if (!($value_checker$)) {\n");
            printer->Indent();
            printer->Print(vars, "__err = \"$field$$kind$ is not $value$\";\n");
            printer->Print(vars, "return false;\n");
            printer->Outdent();
            printer->Print(vars, "}\n");
            break;

        default:
            Y_FAIL_S("Cannot check value of field: type = " << field->type_name());
        }
    }

    void Size(TPrinter& printer) const {
        Y_ABORT_UNLESS(Field->is_repeated(), "Cannot check size of non-repeated field");

        TVariables vars = Vars;
        vars["kind"] = " size";
        vars["getter"] = Field->name() + "_size()";

        CheckLimit(printer, Field->options().GetExtension(Ydb::size), vars);
    }

    void Length(TPrinter& printer) const {
        if (Field->is_repeated()) {
            printer->Print(Vars, "for (const auto& value : $field$()) {\n");
            printer->Indent();
        }

        if (Field->cpp_type() == FieldDescriptor::CPPTYPE_STRING) {
            TVariables vars = Vars;
            vars["kind"] = "'s length";
            vars["getter"] = Field->is_repeated() ? "value.size()" : Field->name() + "().size()";

            CheckLimit(printer, Field->options().GetExtension(Ydb::length), vars);
        } else if (Field->is_map()) {
            const FieldDescriptor* value = Field->message_type()->FindFieldByName("value");
            Y_VERIFY_S(value->cpp_type() == FieldDescriptor::CPPTYPE_STRING, "Cannot check length of field: type = " << value->type_name());

            TVariables vars = Vars;
            vars["kind"] = "'s value length";
            vars["getter"] = "value.second.size()";

            CheckLimit(printer, Field->options().GetExtension(Ydb::length), vars);
        } else {
            Y_FAIL_S("Cannot check length of field: type = " << Field->type_name());
        }

        if (Field->is_repeated()) {
            printer->Outdent();
            printer->Print(Vars, "}\n");
        }
    }

    void MapKey(TPrinter& printer) const {
        Y_ABORT_UNLESS(Field->is_map(), "Cannot validate map key of non-map field");

        printer->Print(Vars, "for (const auto& value : $field$()) {\n");
        printer->Indent();

        TVariables vars = Vars;

        const auto& mapKey = Field->options().GetExtension(Ydb::map_key);
        if (mapKey.has_length()) {
            const FieldDescriptor* key = Field->message_type()->FindFieldByName("key");
            Y_VERIFY_S(key->cpp_type() == FieldDescriptor::CPPTYPE_STRING, "Cannot check length of field: type = " << key->type_name());

            vars["kind"] = "'s key length";
            vars["getter"] = "value.first.size()";
            CheckLimit(printer, mapKey.length(), vars);
        }
        if (mapKey.value()) {
            vars["kind"] = "'s key";
            vars["value"] = SubstGlobalCopy(mapKey.value(), '"', '\'');
            vars["value_checker"] = BuildValueChecker("value.first", mapKey.value());
            CheckValue(printer, Field->message_type()->FindFieldByName("key"), vars);
        }

        printer->Outdent();
        printer->Print(Vars, "}\n");
    }

    void Value(TPrinter& printer) const {
        if (Field->is_repeated()) {
            printer->Print(Vars, "for (const auto& value : $field$()) {\n");
            printer->Indent();
        }

        const TString value = Field->options().GetExtension(Ydb::value);

        TVariables vars = Vars;
        vars["kind"] = "'s value";
        vars["value"] = SubstGlobalCopy(value, '"', '\'');

        if (Field->is_map()) {
            vars["value_checker"] = BuildValueChecker("value.second", value);
            CheckValue(printer, Field->message_type()->FindFieldByName("value"), vars);
        } else {
            vars["value_checker"] = BuildValueChecker(Field->is_repeated() ? "value" : Field->name() + "()", value);
            CheckValue(printer, Field, vars);
        }

        if (Field->is_repeated()) {
            printer->Outdent();
            printer->Print(Vars, "}\n");
        }
    }

    void Body(TPrinter& printer) const {
        const auto& opts = Field->options();

        if (opts.HasExtension(Ydb::required)) {
            Required(printer);
        }

        if (Field->has_presence() && IsScalarType(Field)) {
            printer->Print(Vars, "if (!has_$field$()) {\n");
            printer->Indent();
            printer->Print(Vars, "return true;\n");
            printer->Outdent();
            printer->Print(Vars, "}\n");
        }
        if (opts.HasExtension(Ydb::size)) {
            Size(printer);
        }
        if (opts.HasExtension(Ydb::length)) {
            Length(printer);
        }
        if (opts.HasExtension(Ydb::map_key)) {
            MapKey(printer);
        }
        if (opts.HasExtension(Ydb::value)) {
            Value(printer);
        }

        if (IsCustomMessage(Field->message_type())) {
            if (Field->is_repeated()) {
                printer->Print(Vars, "for (const auto& value : $field$()) {\n");
                printer->Indent();
                printer->Print(Vars, "if (!value.validate(__err)) {\n");
                printer->Indent();
                printer->Print(Vars, "return false;\n");
                printer->Outdent();
                printer->Print(Vars, "}\n");
                printer->Outdent();
                printer->Print(Vars, "}\n");
                printer->Print(Vars, "\nreturn true;\n");
            } else {
                printer->Print(Vars, "return $field$().validate(__err);\n");
            }
        } else {
            printer->Print(Vars, "return true;\n");
        }
    }

    bool HasValidators() const {
        const auto& opts = Field->options();
        return opts.HasExtension(Ydb::required)
            || opts.HasExtension(Ydb::size)
            || opts.HasExtension(Ydb::length)
            || opts.HasExtension(Ydb::map_key)
            || opts.HasExtension(Ydb::value);
    }

    static TString PascalName(TString camelName) {
        camelName.to_upper(0, 1);
        return camelName;
    }

public:
    using TPtr = TIntrusivePtr<TFieldGenerator>;

    explicit TFieldGenerator(const FieldDescriptor* field, const TString& className)
        : Field(field)
        , Vars({
            {"class", className},
            {"func", "validate_" + field->name()},
            {"field", field->name()},
            {"PascalName", PascalName(field->camelcase_name())},
        })
    {
    }

    const TVariables& GetVars() const {
        return Vars;
    }

    void Declare(TPrinter& printer) const {
        if (!HasValidators() && !IsCustomMessage(Field->message_type())) {
            printer->Print(Vars, "bool $func$(TProtoStringType&) const { return true; }\n");
        } else {
            printer->Print(Vars, "bool $func$(TProtoStringType& __err) const;\n");
        }
    }

    void Define(TPrinter& printer) const {
        if (!HasValidators() && !IsCustomMessage(Field->message_type())) {
            return;
        }

        printer->Print(Vars, "bool $class$::$func$(TProtoStringType& __err) const {\n");
        printer->Indent();
        Body(printer);
        printer->Outdent();
        printer->Print(Vars, "}\n\n");
    }

private:
    const FieldDescriptor* Field;
    const TVariables Vars;

}; // TFieldGenerator

class TOneofGenerator: public TThrRefBase {
    void Body(TPrinter& printer) const {
        printer->Print(Vars, "switch ($oneof$_case()) {\n");

        for (auto field : Fields) {
            printer->Print(field->GetVars(), "case k$PascalName$:\n");
            printer->Indent();
            printer->Print(field->GetVars(), "return $func$(__err);\n");
            printer->Outdent();
        }

        printer->Print(Vars, "default:\n");
        printer->Indent();
        printer->Print(Vars, "return true;\n");
        printer->Outdent();

        printer->Print(Vars, "}\n");
    }

public:
    using TPtr = TIntrusivePtr<TOneofGenerator>;

    explicit TOneofGenerator(const OneofDescriptor* oneof, const TString& className)
        : Vars({
            {"class", className},
            {"oneof", oneof->name()},
            {"func", "validate_" + oneof->name()},
        })
    {
    }

    const TVariables& GetVars() const {
        return Vars;
    }

    void AddField(TFieldGenerator::TPtr field) {
        Fields.push_back(field);
    }

    void Declare(TPrinter& printer) const {
        printer->Print(Vars, "bool $func$(TProtoStringType& __err) const;\n");
    }

    void Define(TPrinter& printer) const {
        printer->Print(Vars, "bool $class$::$func$(TProtoStringType& __err) const {\n");
        printer->Indent();
        Body(printer);
        printer->Outdent();
        printer->Print(Vars, "}\n\n");
    }

private:
    const TVariables Vars;
    TVector<TFieldGenerator::TPtr> Fields;

}; // TOneofGenerator

class TMessageGenerator {
    struct TItem {
        TOneofGenerator::TPtr Oneof;
        TVector<TFieldGenerator::TPtr> Fields;

        explicit TItem(TOneofGenerator::TPtr oneof)
            : Oneof(oneof)
        {
        }

        void AddField(TFieldGenerator::TPtr field) {
            Fields.push_back(field);
            if (Oneof) {
                Oneof->AddField(field);
            }
        }
    };

    struct TOrderedCmp {
        bool operator()(const OneofDescriptor* lhs, const OneofDescriptor* rhs) const noexcept {
            if (!lhs && !rhs) {
                return false;
            } else if (!lhs) {
                return true;
            } else if (!rhs) {
                return false;
            }
            return lhs->index() < rhs->index();
        }
    };

    using TItems = TMap<const OneofDescriptor*, TItem, TOrderedCmp>;

    void Declare(TPrinter& printer, const TItems& items) {
        if (!items) {
            printer->Print(Vars, "bool validate(TProtoStringType&) const { return true; }\n");
        } else {
            printer->Print(Vars, "bool validate(TProtoStringType& __err) const;\n");
        }
    }

    void Define(TPrinter& printer, const TItems& items) {
        if (!items) {
            return;
        }

        printer->Print(Vars, "bool $class$::validate(TProtoStringType& __err) const {\n");
        printer->Indent();
        printer->Print(Vars, "return\n");
        printer->Indent();

        bool first = true;
        for (const auto& [_, item] : items) {
            if (!item.Oneof) {
                for (const auto& field : item.Fields) {
                    if (first) {
                        first = false;
                    } else {
                        printer->Print(Vars, "&& ");
                    }
                    printer->Print(field->GetVars(), "$func$(__err)\n");
                }
            } else {
                if (first) {
                    first = false;
                } else {
                    printer->Print(Vars, "&& ");
                }
                printer->Print(item.Oneof->GetVars(), "$func$(__err)\n");
            }
        }

        printer->Outdent();
        printer->Print(Vars, ";\n");
        printer->Outdent();
        printer->Print(Vars, "}\n\n");
    }

public:
    explicit TMessageGenerator(const Descriptor* message, OutputDirectory* output)
        : Message(message)
        , Output(output)
        , Header(output, HeaderFileName(message), ClassScope(message))
        , Source(output, SourceFileName(message), NamespaceScope())
        , Vars({
            {"class", ClassName(message)},
        })
    {
    }

    void Generate() {
        for (auto i = 0; i < Message->nested_type_count(); ++i) {
            const Descriptor* message = Message->nested_type(i);
            if (!IsCustomMessage(message)) {
                continue;
            }

            TMessageGenerator mg(message, Output);
            mg.Generate();
        }

        TItems items;

        for (int i = 0; i < Message->real_oneof_decl_count(); i++) {
            const OneofDescriptor* oneof = Message->oneof_decl(i);
            TOneofGenerator::TPtr oneofGen = new TOneofGenerator(oneof, Vars.at("class"));
            items.emplace(oneof, TItem(oneofGen));
        }

        for (auto i = 0; i < Message->field_count(); ++i) {
            const FieldDescriptor* field = Message->field(i);
            const OneofDescriptor* oneof = field->containing_oneof();

            TFieldGenerator::TPtr fieldGen = new TFieldGenerator(field, Vars.at("class"));

            auto it = items.find(oneof);
            if (it == items.end()) {
                it = items.emplace(nullptr, TItem(nullptr)).first;
            }
            Y_ABORT_UNLESS(it != items.end());
            it->second.AddField(fieldGen);
        }

        for (const auto& [_, item] : items) {
            for (const auto& field : item.Fields) {
                field->Declare(Header);
                field->Define(Source);
            }

            if (item.Oneof) {
                item.Oneof->Declare(Header);
                item.Oneof->Define(Source);
            }
        }

        Declare(Header, items);
        Define(Source, items);
    }

private:
    const Descriptor* Message;
    OutputDirectory* Output;
    TPrinter Header;
    TPrinter Source;
    const TVariables Vars;

}; // TMessageGenerator

class TCodeGenerator: public CodeGenerator {
    bool Generate(
            const FileDescriptor* file,
            const TProtoStringType&,
            OutputDirectory* output,
            TProtoStringType*) const override final {

        for (auto i = 0; i < file->message_type_count(); ++i) {
            TMessageGenerator mg(file->message_type(i), output);
            mg.Generate();
        }

        return true;
    }

    uint64_t GetSupportedFeatures() const override {
        return FEATURE_PROTO3_OPTIONAL;
    }

}; // TCodeGenerator

}

int main(int argc, char* argv[]) {
    NKikimr::NValidation::TCodeGenerator generator;
    return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
