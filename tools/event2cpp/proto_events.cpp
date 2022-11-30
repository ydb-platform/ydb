#include <google/protobuf/compiler/cpp/cpp_helpers.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/stubs/strutil.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor.pb.h>

#include <util/string/cast.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>

#include <library/cpp/eventlog/proto/events_extension.pb.h>

#include "proto_events.h"

namespace NProtoBuf::NCompiler::NPlugins {

namespace NInternal {
    using namespace google::protobuf;
    using namespace google::protobuf::compiler;
    using namespace google::protobuf::compiler::cpp;

    typedef std::map<TProtoStringType, TProtoStringType> TVariables;

    void CheckMessageId(size_t id, const TProtoStringType& name) {
        typedef std::map<size_t, TProtoStringType> TMessageIds;
        TMessageIds* ids = Singleton<TMessageIds>();
        TMessageIds::const_iterator it = ids->find(id);

        if (it != ids->end()) {
            throw yexception() << "Duplicate message_id = " << id
                               << " in messages " << name
                               << " and " << it->second << Endl;
        }

        (*ids)[id] = name;
    }

    void SetCommonFieldVariables(const FieldDescriptor* descriptor, TVariables* variables) {
        (*variables)["rname"] = descriptor->name();
        (*variables)["name"] = FieldName(descriptor);
    }

    TProtoStringType HeaderFileName(const FileDescriptor* file) {
        TProtoStringType basename = cpp::StripProto(file->name());

        return basename.append(".pb.h");
    }

    TProtoStringType SourceFileName(const FileDescriptor* file) {
        TProtoStringType basename = cpp::StripProto(file->name());

        return basename.append(".pb.cc");
    }

    void GeneratePrintingCycle(TVariables vars, TProtoStringType printTemplate, io::Printer* printer) {
        printer->Print("\n{\n");
        printer->Indent();
        printer->Print(vars,
            "NProtoBuf::$repeated_field_type$< $type$ >::const_iterator b = $name$().begin();\n"
            "NProtoBuf::$repeated_field_type$< $type$ >::const_iterator e = $name$().end();\n\n");
        printer->Print("output << \"[\";\n");
        printer->Print("if (b != e) {\n");
        vars["obj"] = "(*b++)";
        printer->Print(vars, printTemplate.c_str());
        printer->Print(";\n");
        printer->Print(vars,
            "for (NProtoBuf::$repeated_field_type$< $type$ >::const_iterator it = b; it != e; ++it) {\n");
        printer->Indent();
        printer->Print("output << \",\";\n");
        vars["obj"] = "(*it)";
        printer->Print(vars, printTemplate.c_str());
        printer->Print(";\n");
        printer->Outdent();
        printer->Print("}\n}\n");
        printer->Print("output << \"]\";\n");
        printer->Outdent();
        printer->Print("}\n");
    }

    class TFieldExtGenerator {
        public:
            TFieldExtGenerator(const FieldDescriptor* field)
                : Descriptor_(field)
            {
                SetCommonFieldVariables(Descriptor_, &Variables_);
            }

            virtual ~TFieldExtGenerator() {
            }

            virtual bool NeedProtobufMessageFieldPrinter() const {
                return false;
            }

            virtual void GenerateCtorArgument(io::Printer* printer) = 0;
            virtual void GenerateInitializer(io::Printer* printer, const TString& prefix) = 0;
            virtual void GeneratePrintingCode(io::Printer* printer) = 0;
        protected:
            const FieldDescriptor* Descriptor_;
            TVariables Variables_;
    };

    class TMessageFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TMessageFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["type"] = ClassName(Descriptor_->message_type(), true);
                Variables_["has_print_function"] = Descriptor_->message_type()->options().HasExtension(message_id) ? "true" : "false";
            }

            bool NeedProtobufMessageFieldPrinter() const override {
                return true;
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    "const $type$& arg_$name$");
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(Variables_,
                    "$prefix$mutable_$name$()->CopyFrom(arg_$name$);\n");
            }

            void GeneratePrintingCode(io::Printer* printer) override {
                printer->Print("output << \"{\";\n");
                printer->Print(Variables_,
                    "protobufMessageFieldPrinter.PrintProtobufMessageFieldToOutput<$type$, $has_print_function$>($name$(), escapedOutput);\n");
                printer->Print("output << \"}\";\n");
            }
    };

    class TMapFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TMapFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
            }

            void GenerateCtorArgument(io::Printer* /* printer */) override {
            }

            void GenerateInitializer(io::Printer* /* printer */, const TString& /* prefix */) override {
            }

            void GeneratePrintingCode(io::Printer* /* printer */) override {
            }
    };

    class TRepeatedMessageFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TRepeatedMessageFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["type"] = ClassName(Descriptor_->message_type(), true);
                Variables_["repeated_field_type"] = "RepeatedPtrField";
                Variables_["has_print_function"] = Descriptor_->message_type()->options().HasExtension(message_id) ? "true" : "false";
            }

            bool NeedProtobufMessageFieldPrinter() const override {
                return true;
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    "const $type$& arg_$name$");
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(Variables_,
                    "$prefix$add_$name$()->CopyFrom(arg_$name$);\n");
            }
            void GeneratePrintingCode(io::Printer* printer) override {
                GeneratePrintingCycle(Variables_, "protobufMessageFieldPrinter.PrintProtobufMessageFieldToOutput<$type$, $has_print_function$>($obj$, escapedOutput)", printer);
            }
    };

    class TStringFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TStringFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["pointer_type"] = Descriptor_->type() == FieldDescriptor::TYPE_BYTES ? "void" : "char";
                Variables_["type"] = "TProtoStringType";
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    (Descriptor_->type() == FieldDescriptor::TYPE_BYTES ?
                        "const $pointer_type$* arg_$name$, size_t arg_$name$_size" : "const $type$& arg_$name$")
                    );
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(
                    Variables_,
                    Descriptor_->type() == FieldDescriptor::TYPE_BYTES ?
                        "$prefix$set_$name$(arg_$name$, arg_$name$_size);\n" :
                        "$prefix$set_$name$(arg_$name$);\n"
                );
            }

            void GeneratePrintingCode(io::Printer* printer) override {
                Repr::ReprType fmt = Repr::none;

                if (Descriptor_->options().HasExtension(repr)) {
                    fmt = Descriptor_->options().GetExtension(repr);
                }

                switch (fmt) {
                    case Repr::as_base64:
                        printer->Print(Variables_, "NProtoBuf::PrintAsBase64($name$(), output);\n");
                        break;

                    case Repr::none:
                    /* TODO: proper error handling?*/
                    default:
                        printer->Print(Variables_, "escapedOutput << $name$();\n");
                        break;
                }
            }
    };

    class TRepeatedStringFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TRepeatedStringFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["pointer_type"] = Descriptor_->type() == FieldDescriptor::TYPE_BYTES ? "void" : "char";
                Variables_["type"] = "TProtoStringType";
                Variables_["repeated_field_type"] = "RepeatedPtrField";
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    (Descriptor_->type() == FieldDescriptor::TYPE_BYTES ?
                        "const $pointer_type$* arg_$name$, size_t arg_$name$_size": "const $type$& arg_$name$")
                    );
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(
                    Variables_,
                    Descriptor_->type() == FieldDescriptor::TYPE_BYTES ?
                        "$prefix$add_$name$(arg_$name$, arg_$name$_size);\n" :
                        "$prefix$add_$name$(arg_$name$);\n"
                );
            }
            void GeneratePrintingCode(io::Printer* printer) override {
                GeneratePrintingCycle(Variables_, "output << \"\\\"\" << $obj$ << \"\\\"\"", printer);
            }
    };

    class TEnumFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TEnumFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["type"] = ClassName(Descriptor_->enum_type(), true);
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    "$type$ arg_$name$");
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(Variables_,
                    "$prefix$set_$name$(arg_$name$);\n");
            }

            void GeneratePrintingCode(io::Printer* printer) override {
                printer->Print(Variables_,
                    "output << $type$_Name($name$());\n");
            }
    };

    class TRepeatedEnumFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TRepeatedEnumFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["type"] = ClassName(Descriptor_->enum_type(), true);
                Variables_["repeated_field_type"] = "RepeatedField";
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    "$type$ arg_$name$");
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(Variables_,
                    "$prefix$add_$name$(arg_$name$);\n");
            }

            void GeneratePrintingCode(io::Printer* printer) override {
                TStringStream pattern;

                TProtoStringType type = Variables_["type"];
                pattern << "output << " << type << "_Name(" << type << "($obj$))";
                Variables_["type"] = "int";
                GeneratePrintingCycle(Variables_, pattern.Str(), printer);
                Variables_["type"] = type;
            }
    };

    class TPrimitiveFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TPrimitiveFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["type"] = PrimitiveTypeName(Descriptor_->cpp_type());
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    "$type$ arg_$name$");
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(Variables_,
                    "$prefix$set_$name$(arg_$name$);\n");
            }

            void GeneratePrintingCode(io::Printer* printer) override {
                Repr::ReprType fmt = Repr::none;

                if (Descriptor_->options().HasExtension(repr)) {
                    fmt = Descriptor_->options().GetExtension(repr);
                }

                switch (fmt) {
                    case Repr::as_bytes:
                        printer->Print(Variables_, "NProtoBuf::PrintAsBytes($name$(), output);\n");
                        break;

                    case Repr::as_hex:
                        printer->Print(Variables_, "NProtoBuf::PrintAsHex($name$(), output);\n");
                        break;

                    case Repr::none:
                    /* TODO: proper error handling? */
                    default:
                        printer->Print(Variables_, "output << $name$();\n");
                        break;
                }
            }
    };

    class TRepeatedPrimitiveFieldExtGenerator: public TFieldExtGenerator  {
        public:
            TRepeatedPrimitiveFieldExtGenerator(const FieldDescriptor* field)
                : TFieldExtGenerator(field)
            {
                Variables_["type"] = PrimitiveTypeName(Descriptor_->cpp_type());
                Variables_["repeated_field_type"] = "RepeatedField";
            }

            void GenerateCtorArgument(io::Printer* printer) override {
                printer->Print(Variables_,
                    "$type$ arg_$name$");
            }

            void GenerateInitializer(io::Printer* printer, const TString& prefix) override {
                Variables_["prefix"] = prefix;
                printer->Print(Variables_,
                    "$prefix$add_$name$(arg_$name$);\n");
            }

            void GeneratePrintingCode(io::Printer* printer) override {
                GeneratePrintingCycle(Variables_, "output << $obj$", printer);
            }
    };

    std::unique_ptr<TFieldExtGenerator> MakeGenerator(const FieldDescriptor* field) {
        if (field->is_map()) {
            return std::make_unique<TMapFieldExtGenerator>(field);
        } else if (field->is_repeated()) {
            switch (field->cpp_type()) {
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    return std::make_unique<TRepeatedMessageFieldExtGenerator>(field);
                case FieldDescriptor::CPPTYPE_STRING:
                    switch (field->options().ctype()) {
                        default:  // RepeatedStringFieldExtGenerator handles unknown ctypes.
                        case FieldOptions::STRING:
                            return std::make_unique<TRepeatedStringFieldExtGenerator>(field);
                    }
                case FieldDescriptor::CPPTYPE_ENUM:
                    return std::make_unique<TRepeatedEnumFieldExtGenerator>(field);
                default:
                    return std::make_unique<TRepeatedPrimitiveFieldExtGenerator>(field);
            }
        } else {
            switch (field->cpp_type()) {
                case FieldDescriptor::CPPTYPE_MESSAGE:
                    return std::make_unique<TMessageFieldExtGenerator>(field);
                case FieldDescriptor::CPPTYPE_STRING:
                    switch (field->options().ctype()) {
                        default:  // StringFieldGenerator handles unknown ctypes.
                        case FieldOptions::STRING:
                            return std::make_unique<TStringFieldExtGenerator>(field);
                    }
                case FieldDescriptor::CPPTYPE_ENUM:
                    return std::make_unique<TEnumFieldExtGenerator>(field);
                default:
                    return std::make_unique<TPrimitiveFieldExtGenerator>(field);
            }
        }
    }

    class TMessageExtGenerator {
        public:
            TMessageExtGenerator(const Descriptor* descriptor, OutputDirectory* outputDirectory)
                : Descriptor_(descriptor)
                , HasMessageId_(Descriptor_->options().HasExtension(message_id))
                , ClassName_(ClassName(Descriptor_, false))
                , OutputDirectory_(outputDirectory)
                , HasGeneratorWithProtobufMessageFieldPrinter_(false)
                , CanGenerateSpecialConstructor_(false)
            {
                NestedGenerators_.reserve(descriptor->nested_type_count());
                for (int i = 0; i < descriptor->nested_type_count(); i++) {
                    NestedGenerators_.emplace_back(descriptor->nested_type(i), OutputDirectory_);
                }

                if (HasMessageId_) {
                    FieldGenerators_.reserve(descriptor->field_count());
                    for (int i = 0; i < descriptor->field_count(); i++) {
                        FieldGenerators_.emplace_back(MakeGenerator(descriptor->field(i)));
                        HasGeneratorWithProtobufMessageFieldPrinter_ |= FieldGenerators_.back()->NeedProtobufMessageFieldPrinter();
                    }
                }

                {
                    size_t intFieldCount = 0;
                    size_t mapFieldCount = 0;
                    size_t nonMapFieldCount = 0;
                    for (int i = 0; i < Descriptor_->field_count(); ++i) {
                        const FieldDescriptor* field = Descriptor_->field(i);
                        if (field->is_map()) {
                            ++mapFieldCount;
                        } else {
                            ++nonMapFieldCount;
                        }
                        switch (field->cpp_type()) {
                            case FieldDescriptor::CPPTYPE_INT32:
                            case FieldDescriptor::CPPTYPE_INT64:
                            case FieldDescriptor::CPPTYPE_UINT32:
                            case FieldDescriptor::CPPTYPE_UINT64:
                                ++intFieldCount;
                                break;
                            default:
                                break;
                        }
                    }

                    CanGenerateSpecialConstructor_ = (
                        // Certain field combinations would result in ambiguous constructor generation.
                        // Do not generate special contructor for such combinations.
                        (intFieldCount != nonMapFieldCount || nonMapFieldCount != 2) &&

                        // Generate special contructor only if there is at least one non-map Field.
                        nonMapFieldCount > 0
                    );
                }

            }

            void GenerateClassDefinitionExtension() {
                if (Descriptor_->options().HasExtension(realm_name) || Descriptor_->options().HasExtension(message_id)) {
                    GeneratePseudonim();
                }

                if (!HasMessageId_) {
                    return;
                }

                CheckMessageId(Descriptor_->options().GetExtension(message_id), ClassName_);

                TProtoStringType fileName = HeaderFileName(Descriptor_->file());
                TProtoStringType scope = "class_scope:" + Descriptor_->full_name();
                std::unique_ptr<io::ZeroCopyOutputStream> output(
                    OutputDirectory_->OpenForInsert(fileName, scope));
                io::Printer printer(output.get(), '$');

                printer.Print("//Yandex events extension.\n");
                GenerateHeaderImpl(&printer);

                for (auto& nestedGenerator: NestedGenerators_) {
                    nestedGenerator.GenerateClassDefinitionExtension();
                }
            }

            bool GenerateClassExtension() {
                TProtoStringType fileName = SourceFileName(Descriptor_->file());
                std::unique_ptr<io::ZeroCopyOutputStream> output(
                    OutputDirectory_->OpenForInsert(fileName, "namespace_scope"));
                io::Printer printer(output.get(), '$');

                bool hasEventExtension = GenerateSourceImpl(&printer);

                for (auto& nestedGenerator: NestedGenerators_) {
                    hasEventExtension |= nestedGenerator.GenerateSourceImpl(&printer);
                }

                return hasEventExtension;
            }

            void GenerateRegistration(io::Printer* printer) {
                if (!HasMessageId_) {
                    return;
                }

                TVariables vars;
                vars["classname"] = ClassName_;

                printer->Print(vars, "NProtoBuf::TEventFactory::Instance()->RegisterEvent($classname$::descriptor()->options().GetExtension(message_id), factory->GetPrototype($classname$::descriptor()), $classname$::Print);\n");
            }

        private:
            void GenerateHeaderImpl(io::Printer* printer) {
                TVariables vars;
                TProtoStringType mId(ToString(Descriptor_->options().GetExtension(message_id)));
                vars["classname"] = ClassName_;
                vars["messageid"] = mId.data();
                vars["superclass"] = SuperClassName(Descriptor_, Options{});

                printer->Print(vars,
                    "enum {ID = $messageid$};\n\n");

                {
                    /*
                     * Unconditionally generate FromFields() factory method,
                     * so it could be used in template code
                     */
                    printer->Print(vars, "static $classname$ FromFields(\n");
                    GenerateCtorArgs(printer);
                    printer->Print(");\n");
                }

                if (CanGenerateSpecialConstructor_) {
                    printer->Print(vars, "$classname$(\n");
                    GenerateCtorArgs(printer);
                    printer->Print(");\n");
                }

                {
                    printer->Print("void Print(IOutputStream& output, EFieldOutputFlags outputFlags = {}) const;\n");
                    printer->Print("static void Print(const google::protobuf::Message* ev, IOutputStream& output, EFieldOutputFlags outputFlags = {});\n");
                }
            }

            void GeneratePseudonim() {
                TProtoStringType fileName = HeaderFileName(Descriptor_->file());
                std::unique_ptr<io::ZeroCopyOutputStream> output(
                    OutputDirectory_->OpenForInsert(fileName, "namespace_scope"));
                io::Printer printer(output.get(), '$');

                std::vector<TProtoStringType> realm_parts;

                if (Descriptor_->options().HasExtension(realm_name)) {
                    SplitStringUsing(Descriptor_->options().GetExtension(realm_name), ".", &realm_parts);
                }

                if (realm_parts.size() > 0) printer.Print("\n");

                for (size_t i = 0; i < realm_parts.size(); ++i) {
                    printer.Print("namespace $part$ {\n",
                        "part", realm_parts[i]);
                }

                printer.Print("typedef $fullclassname$ T$classname$;\n",
                    "fullclassname", FullClassName(Descriptor_),
                    "classname", ClassName_);

                for (size_t i = realm_parts.size(); i > 0; --i) {
                    printer.Print("}  // namespace $part$\n",
                        "part", realm_parts[i - 1]);
                }
            }

            TProtoStringType FullClassName(const Descriptor* descriptor) {
                TProtoStringType result;
                std::vector<TProtoStringType> parts;

                SplitStringUsing(descriptor->file()->package(), ".", &parts);
                for (size_t i = 0; i < parts.size(); ++i) {
                    result += "::" + parts[i];
                }

                result += "::" + ClassName(descriptor, false);

                return result;
            }

            bool GenerateSourceImpl(io::Printer* printer) {
                if (!HasMessageId_) {
                    return false;
                }

                TVariables vars;
                vars["classname"] = ClassName_;

                {
                    // Generate static $classname$::FromFields impl.
                    printer->Print(vars, "$classname$ $classname$::FromFields(\n");
                    GenerateCtorArgs(printer);
                    printer->Print(")\n");

                    printer->Print("{\n");

                    printer->Indent();
                    printer->Print(vars, "$classname$ result;\n");
                    GenerateFieldInitializers(printer, /* prefix = */ "result.");
                    printer->Print("return result;\n");
                    printer->Outdent();

                    printer->Print("}\n\n");
                }

                if (CanGenerateSpecialConstructor_) {
                    // Generate special constructor impl.
                    printer->Print(vars, "$classname$::$classname$(\n");
                    GenerateCtorArgs(printer);
                    printer->Print(")\n");

                    printer->Print("{\n");

                    printer->Indent();
                    printer->Print("SharedCtor();\n");
                    GenerateFieldInitializers(printer, /* prefix = */ "");
                    printer->Outdent();

                    printer->Print("}\n\n");
                }

                {
                    // Generate $classname$::Print impl.
                    const size_t fieldCount = Descriptor_->field_count();
                    if (fieldCount > 0) {
                        printer->Print(vars,
                            "void $classname$::Print(IOutputStream& output, EFieldOutputFlags outputFlags) const {\n");
                        printer->Indent();
                        printer->Print(
                            "TEventFieldOutput escapedOutput{output, outputFlags};\n"
                            "Y_UNUSED(escapedOutput);\n");

                        if (HasGeneratorWithProtobufMessageFieldPrinter_) {
                            printer->Print(
                                "TEventProtobufMessageFieldPrinter protobufMessageFieldPrinter(EProtobufMessageFieldPrintMode::DEFAULT);\n");
                        }
                    } else {
                        printer->Print(vars,
                            "void $classname$::Print(IOutputStream& output, EFieldOutputFlags) const {\n");
                        printer->Indent();
                    }

                    printer->Print(vars,
                        "output << \"$classname$\";\n");

                    for (size_t i = 0; i < fieldCount; ++i) {
                        printer->Print("output << \"\\t\";\n");
                        FieldGenerators_[i]->GeneratePrintingCode(printer);
                    }

                    printer->Outdent();
                    printer->Print("}\n\n");
                }

                {
                    // Generate static $classname$::Print impl.
                    printer->Print(vars,
                        "void $classname$::Print(const google::protobuf::Message* ev, IOutputStream& output, EFieldOutputFlags outputFlags) {\n");
                    printer->Indent();
                    printer->Print(vars,
                        "const $classname$* This(CheckedCast<const $classname$*>(ev));\n");
                    printer->Print(
                        "This->Print(output, outputFlags);\n");
                    printer->Outdent();
                    printer->Print("}\n\n");
                }

                return true;
            }

            void GenerateCtorArgs(io::Printer* printer) {
                printer->Indent();
                const size_t fieldCount = Descriptor_->field_count();
                bool isFirst = true;
                for (size_t i = 0; i < fieldCount; ++i) {
                    if (Descriptor_->field(i)->is_map()) {
                        continue;
                    }
                    const char* delimiter = isFirst ? "" : ", ";
                    isFirst = false;
                    printer->Print(delimiter);
                    FieldGenerators_[i]->GenerateCtorArgument(printer);
                }
                printer->Outdent();
            }

            void GenerateFieldInitializers(io::Printer* printer, const TString& prefix) {
                for (auto& fieldGeneratorHolder: FieldGenerators_) {
                    fieldGeneratorHolder->GenerateInitializer(printer, prefix);
                }
            }

        private:
            const Descriptor* Descriptor_;
            const bool HasMessageId_;
            TProtoStringType ClassName_;
            OutputDirectory* OutputDirectory_;
            bool HasGeneratorWithProtobufMessageFieldPrinter_;
            bool CanGenerateSpecialConstructor_;
            std::vector<std::unique_ptr<TFieldExtGenerator>>  FieldGenerators_;
            std::vector<TMessageExtGenerator> NestedGenerators_;
    };

    class TFileExtGenerator {
        public:
            TFileExtGenerator(const FileDescriptor* file, OutputDirectory* output_directory)
                : OutputDirectory_(output_directory)
                , File_(file)
            {
                MessageGenerators_.reserve(file->message_type_count());
                for (int i = 0; i < file->message_type_count(); i++) {
                    MessageGenerators_.emplace_back(file->message_type(i), OutputDirectory_);
                }
            }

            void GenerateHeaderExtensions() {
                TProtoStringType fileName = HeaderFileName(File_);

                std::unique_ptr<io::ZeroCopyOutputStream> output(
                    OutputDirectory_->OpenForInsert(fileName, "includes"));
                io::Printer printer(output.get(), '$');

                printer.Print("#include <library/cpp/eventlog/event_field_output.h>\n");
                printer.Print("#include <library/cpp/eventlog/event_field_printer.h>\n");

                for (auto& messageGenerator: MessageGenerators_) {
                    messageGenerator.GenerateClassDefinitionExtension();
                }
            }

            void GenerateSourceExtensions() {
                TProtoStringType fileName = SourceFileName(File_);

                std::unique_ptr<io::ZeroCopyOutputStream> output(
                    OutputDirectory_->OpenForInsert(fileName, "includes"));
                io::Printer printer(output.get(), '$');
                printer.Print("#include <google/protobuf/io/printer.h>\n");
                printer.Print("#include <google/protobuf/io/zero_copy_stream_impl_lite.h>\n");
                printer.Print("#include <google/protobuf/stubs/strutil.h>\n");
                printer.Print("#include <library/cpp/eventlog/events_extension.h>\n");
                printer.Print("#include <util/generic/cast.h>\n");
                printer.Print("#include <util/stream/output.h>\n");

                bool hasEventExtension = false;

                for (auto& messageGenerator: MessageGenerators_) {
                    hasEventExtension |= messageGenerator.GenerateClassExtension();
                }

                if (hasEventExtension) {
                    GenerateEventRegistrations();
                }
            }

            void GenerateEventRegistrations() {
                TVariables vars;
                TProtoStringType fileId = FilenameIdentifier(File_->name());
                vars["regfunction"] = "regevent_" + fileId;
                vars["regclassname"] = "TRegister_" + fileId;
                vars["regvarname"] = "registrator_" + fileId ;
                vars["filename"] = File_->name();

                {
                    TProtoStringType fileName = SourceFileName(File_);
                    std::unique_ptr<io::ZeroCopyOutputStream> output(
                        OutputDirectory_->OpenForInsert(fileName, "namespace_scope"));
                    io::Printer printer(output.get(), '$');

                    GenerateRegistrationFunction(vars, printer);
                    GenerateRegistratorDefinition(vars, printer);
                }

                {

                    TProtoStringType fileName = HeaderFileName(File_);
                    std::unique_ptr<io::ZeroCopyOutputStream> output(
                        OutputDirectory_->OpenForInsert(fileName, "namespace_scope"));
                    io::Printer printer(output.get(), '$');
                    GenerateRegistratorDeclaration(vars, printer);
                }
            }

            void GenerateRegistrationFunction(const TVariables& vars, io::Printer& printer) {
                printer.Print(vars,
                    "void $regfunction$() {\n");
                printer.Indent();

                printer.Print("google::protobuf::MessageFactory* factory = google::protobuf::MessageFactory::generated_factory();\n\n");
                for (auto& messageGenerator: MessageGenerators_) {
                    messageGenerator.GenerateRegistration(&printer);
                }
                printer.Outdent();
                printer.Print("}\n\n");
            }

            void GenerateRegistratorDeclaration(const TVariables& vars, io::Printer& printer) {
                printer.Print(vars, "\nclass $regclassname$ {\n");
                printer.Print("public:\n");
                printer.Indent();
                printer.Print(vars, "$regclassname$();\n");
                printer.Outdent();
                printer.Print("private:\n");
                printer.Indent();
                printer.Print("static bool Registered;\n");
                printer.Outdent();
                printer.Print(vars, "};\n");
                printer.Print(vars, "static $regclassname$ $regvarname$;\n\n");
            }

            void GenerateRegistratorDefinition(const TVariables& vars, io::Printer& printer) {
                printer.Print(vars, "$regclassname$::$regclassname$() {\n");
                printer.Indent();
                printer.Print("if (!Registered) {\n");
                printer.Indent();
                printer.Print(vars, "NProtoBuf::TEventFactory::Instance()->ScheduleRegistration(&$regfunction$);\n");
                printer.Print("Registered = true;\n");
                printer.Outdent();
                printer.Print("}\n");
                printer.Outdent();
                printer.Print("}\n\n");
                printer.Print(vars, "bool $regclassname$::Registered;\n\n");
            }
        private:
            OutputDirectory* OutputDirectory_;
            const FileDescriptor* File_;
            std::vector<TMessageExtGenerator> MessageGenerators_;
    };
}

    bool TProtoEventExtensionGenerator::Generate(const google::protobuf::FileDescriptor* file,
        const TProtoStringType& parameter,
        google::protobuf::compiler::OutputDirectory* outputDirectory,
        TProtoStringType* error) const {
        Y_UNUSED(parameter);
        Y_UNUSED(error);

        NInternal::TFileExtGenerator fileGenerator(file, outputDirectory);

        // Generate header.
        fileGenerator.GenerateHeaderExtensions();

        // Generate cc file.
        fileGenerator.GenerateSourceExtensions();

        return true;
    }

} // namespace NProtoBuf::NCompiler::NPlugins

int main(int argc, char* argv[]) {
#ifdef _MSC_VER
    // Don't print a silly message or stick a modal dialog box in my face,
    // please.
    _set_abort_behavior(0u, ~0u);
#endif  // !_MSC_VER

    try {
        NProtoBuf::NCompiler::NPlugins::TProtoEventExtensionGenerator generator;
        return google::protobuf::compiler::PluginMain(argc, argv, &generator);
    } catch (yexception& e) {
        Cerr << e.what() << Endl;
    } catch (...) {
        Cerr << "Unknown error in TProtoEventExtensionGenerator" << Endl;
    }

    return 1;
}
