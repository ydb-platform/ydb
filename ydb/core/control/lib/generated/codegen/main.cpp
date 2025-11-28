#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tablet.pb.h>

#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/subst.h>
#include <util/string/vector.h>
#include <util/system/src_location.h>

#include <google/protobuf/descriptor.pb.h>

#include <jinja2cpp/template_env.h>
#include <jinja2cpp/template.h>
#include <jinja2cpp/value.h>
#include <jinja2cpp/reflected_value.h>

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

struct TImmediateControl {
    TString Name;
    TMaybe<TString> FullPath;
    TMaybe<TString> HtmlName;
    TMaybe<uint64_t> MinValue;
    TMaybe<uint64_t> MaxValue;
    TMaybe<uint64_t> DefaultValue;
    bool IsConfigBasedControl = true;
};

struct TImmediateControlsClass {
    TString Name;
    TString ClassName;
    TMaybe<TString> ProtoConfigClassName;
    std::vector<const TImmediateControlsClass*> InnerClasses;
    std::vector<const TImmediateControl*> Controls;
};


namespace jinja2 {
    Value Reflect(TMaybe<uint64_t> val) {
        if (val) {
            return Value(static_cast<int64_t>(*val));
        } else {
            return Value();
        }
    }

    template<>
    struct TypeReflection<TImmediateControl> : TypeReflected<TImmediateControl> {
        static const auto& GetAccessors() {
            static std::unordered_map<std::string, FieldAccessor> accessors = {
                {"name", [](const TImmediateControl& control) { return Reflect(std::string(control.Name)); }},
                {"full_path", [](const TImmediateControl& control) { return Reflect(std::string(*control.FullPath)); }},
                {"has_full_path", [](const TImmediateControl& control) { return Reflect(control.FullPath.Defined()); }},
                {"html_name", [](const TImmediateControl& control) { return Reflect(std::string(*control.HtmlName)); }},
                {"has_html_name", [](const TImmediateControl& control) { return Reflect(control.HtmlName.Defined()); }},
                {"is_config_based_control", [](const TImmediateControl& control) { return Reflect(control.IsConfigBasedControl); }},
                {"max_value", [](const TImmediateControl& control) { return Reflect(control.MaxValue); }},
                {"min_value", [](const TImmediateControl& control) { return Reflect(control.MinValue); }},
                {"default_value", [](const TImmediateControl& control) { return Reflect(control.DefaultValue); }}
            };
            return accessors;
        }
    };

    template<>
    struct TypeReflection<TImmediateControlsClass> : TypeReflected<TImmediateControlsClass> {
        static const auto& GetAccessors() {
            static std::unordered_map<std::string, FieldAccessor> accessors = {
                {"name", [](const TImmediateControlsClass& controlClass) { return Reflect(std::string(controlClass.Name)); }},
                {"proto_config_class_name", [](const TImmediateControlsClass& controlClass) { return Reflect(std::string(*controlClass.ProtoConfigClassName)); }},
                {"has_proto_config_class_name", [](const TImmediateControlsClass& controlClass) { return Reflect(controlClass.ProtoConfigClassName.Defined()); }},
                {"proto_config_class_short_name", [](const TImmediateControlsClass& controlClass) {
                    auto fullName = TStringBuf{*controlClass.ProtoConfigClassName};
                    fullName.SkipPrefix("NKikimrConfig::");
                    return Reflect(std::string(fullName));
                }},
                {"class_name", [](const TImmediateControlsClass& controlClass) { return Reflect(std::string(controlClass.ClassName)); }},
                {"inner_classes", [](const TImmediateControlsClass& controlClass) { return Reflect(controlClass.InnerClasses); }},
                {"controls", [](const TImmediateControlsClass& controlClass) { return Reflect(controlClass.Controls); }}
            };
            return accessors;
        }
    };
} // namespace jinja2

struct TCodeGenContext {
    std::deque<TImmediateControlsClass> ControlClasses;
    std::deque<TImmediateControl> Controls;
    std::vector<const TImmediateControlsClass*> JinjaControlClasses;
    std::vector<const TImmediateControl*> JinjaControls;
    std::vector<const TImmediateControlsClass*> JinjaRootControlClasses;
};

TMaybe<TString> GetControlFullPath(TVector<TString> fullPathInHierarchy) {
    auto res = JoinStrings(fullPathInHierarchy, ".");
    if (res.Contains("GRpcControls")) {
        return Nothing();
    }
    return res;
}

const TImmediateControl* CodeGenControl(const ::google::protobuf::FieldDescriptor& protoControl, TVector<TString> fullPathInHierarchy, TCodeGenContext& context) {
    fullPathInHierarchy.push_back(protoControl.name());
    auto fullPath = GetControlFullPath(std::move(fullPathInHierarchy));
    TImmediateControl control {
        .Name = protoControl.name(),
        .FullPath = fullPath,
        .HtmlName = fullPath
    };
    if (protoControl.options().HasExtension(NKikimrConfig::ControlOptions)) {
        const auto& controlOptions = protoControl.options().GetExtension(NKikimrConfig::ControlOptions);
        control.MaxValue = (controlOptions.HasMaxValue()) ? controlOptions.GetMaxValue() : TMaybe<uint64_t>{};
        control.MinValue = (controlOptions.HasMinValue()) ? controlOptions.GetMinValue() : TMaybe<uint64_t>{};
        control.DefaultValue = (controlOptions.HasDefaultValue()) ? controlOptions.GetDefaultValue() : TMaybe<uint64_t>{};
    }
    auto* res = &context.Controls.emplace_back(std::move(control));
    context.JinjaControls.push_back(res);
    return res;
}

template <typename TMapHandler, typename TCodeGenClass>
const TImmediateControlsClass* CodeGenClassImpl(
    const ::google::protobuf::Descriptor& protoClassDescriptor,
    TMapHandler mapHandler, TCodeGenClass codeGenClass,
    TVector<TString> fullPathInHierarchy,
    TImmediateControlsClass& controlClass, TCodeGenContext& context) {

    for (int fieldIndex = 0; fieldIndex < protoClassDescriptor.field_count(); ++fieldIndex) {
        const auto* innerField = protoClassDescriptor.field(fieldIndex);
        if (innerField->is_map()) {
            mapHandler(innerField);
        } else if (EqualToOneOf(innerField->type(), ::google::protobuf::FieldDescriptor::TYPE_UINT64, ::google::protobuf::FieldDescriptor::TYPE_INT64)) {
            controlClass.Controls.emplace_back(CodeGenControl(*innerField, fullPathInHierarchy, context));
        } else if (innerField->type() == ::google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
            controlClass.InnerClasses.emplace_back(codeGenClass(*innerField, fullPathInHierarchy, context));
        }
    }

    auto* res = &context.ControlClasses.emplace_back(std::move(controlClass));
    context.JinjaControlClasses.push_back(res);
    return res;
}

static const TString RequestConfigsClassName = "TImmediateRequestConfigControls";

const TImmediateControlsClass* CodeGenRequestConfigsForService(const TString& serviceName, TCodeGenContext& context) {
    auto protoClassDescriptor = NKikimrConfig::TImmediateControlsConfig_TGRpcControls_TRequestConfig::descriptor();
    for (int fieldIndex = 0; fieldIndex < protoClassDescriptor->field_count(); ++fieldIndex) {
        const auto* innerField = protoClassDescriptor->field(fieldIndex);
        auto control = *CodeGenControl(*innerField, TVector<TString>{"GRpcControls", "RequestConfigs"}, context);
        context.Controls.pop_back();
        context.JinjaControls.pop_back();
        control.FullPath = "GRpcControls.RequestConfigs." + serviceName + "." + innerField->name();
        control.HtmlName = control.FullPath;
        auto emplacedControlPtr = &context.Controls.emplace_back(std::move(control));
        context.JinjaControls.push_back(emplacedControlPtr);
    }

    TImmediateControlsClass controlClass{.Name = serviceName, .ClassName = RequestConfigsClassName };
    return &context.ControlClasses.emplace_back(std::move(controlClass)); // don't need to register in jinja since it's a proxy class
}

void CodeGenRequestConfigsInner(TCodeGenContext& context);

std::vector<TString> GetRequestConfigsServices() {
    return {
        "CoordinationService_Session",
        "ClickhouseInternal_Scan",
        "ClickhouseInternal_GetShardLocations",
        "ClickhouseInternal_DescribeTable",
        "ClickhouseInternal_CreateSnapshot",
        "ClickhouseInternal_RefreshSnapshot",
        "ClickhouseInternal_DiscardSnapshot",
        "TableService_StreamExecuteScanQuery",
        "TableService_StreamReadTable",
        "TableService_ReadRows",
        "FooBar"
    };
}

const TImmediateControlsClass* CodeGenAllRequestConfigs(TCodeGenContext& context) {
    TImmediateControlsClass controlClass{.Name = "RequestConfigs", .ClassName = "TRequestConfigsOuter" };
    CodeGenRequestConfigsInner(context); //important to call first
    for (const auto& service: GetRequestConfigsServices()) {
        controlClass.InnerClasses.emplace_back(CodeGenRequestConfigsForService(service, context));
    }
    auto* res = &context.ControlClasses.emplace_back(std::move(controlClass));
    context.JinjaControlClasses.push_back(res);
    return res;
}

TString GetConfigFullName(const TString& protoFullname) {
    TString res = protoFullname;
    SubstGlobal(res, "NKikimrConfig.", "NKikimrConfig::");
    SubstGlobal(res, ".", "_");
    return res;
}

const TImmediateControlsClass* CodeGenClass(const ::google::protobuf::FieldDescriptor& protoClass, TVector<TString> fullPathInHierarchy, TCodeGenContext& context, bool registerInRoot = false) {
    TImmediateControlsClass controlClass;
    controlClass.Name = protoClass.name();
    const auto* protoClassDescriptor = protoClass.message_type();
    controlClass.ClassName = protoClassDescriptor->name();
    controlClass.ProtoConfigClassName = GetConfigFullName(protoClassDescriptor->full_name());
    fullPathInHierarchy.push_back(controlClass.Name);

    auto res = CodeGenClassImpl(
        *protoClassDescriptor,
        [&](const auto* innerField) {
            Y_ENSURE(innerField->name() == "RequestConfigs" && protoClass.name() == "GRpcControls");
            controlClass.InnerClasses.emplace_back(CodeGenAllRequestConfigs(context));
        },
        std::bind(CodeGenClass, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, false),
        std::move(fullPathInHierarchy),
        controlClass,
        context
    );
    if (registerInRoot) {
        context.JinjaRootControlClasses.push_back(res);
    }
    return res;
}


void CodeGenRequestConfigsInner(TCodeGenContext& context) {
    TImmediateControlsClass controlClass {
        .Name = "RequestConfigs",
        .ClassName = RequestConfigsClassName
    };
    auto protoClassDescriptor = NKikimrConfig::TImmediateControlsConfig_TGRpcControls_TRequestConfig::descriptor();
    controlClass.ProtoConfigClassName = GetConfigFullName(protoClassDescriptor->full_name());

    CodeGenClassImpl(
        *protoClassDescriptor,
        [&](const auto*) {},
        std::bind(CodeGenClass, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, false),
        TVector<TString>{"GRpcControls", "RequestConfigs"},
        controlClass,
        context
    );
}

void CodeGenControlsForTablets(TCodeGenContext& context) {
    auto tabletTypesDescr = NKikimrTabletBase::TTabletTypes::descriptor();
    Y_ENSURE(tabletTypesDescr->enum_type_count() == 1);
    auto enumDescr = tabletTypesDescr->enum_type(0);
    for (int enumIndex = 0; enumIndex < enumDescr->value_count(); ++enumIndex) {
        auto enumValue = enumDescr->value(enumIndex);
        if (enumValue->number() == NKikimrTabletBase::TTabletTypes::UserTypeStart) {
            break;
        }
        TImmediateControl control {
            .Name = {},
            .FullPath = TStringBuilder() << "LogFlushDelayOverrideUsec[" <<  ToString(enumValue->number())  <<  "]",
            .HtmlName = TStringBuilder() << enumValue->name() << "_LogFlushDelayOverrideUsec",
            .IsConfigBasedControl = false
        };
        auto emplacedControlPtr = &context.Controls.emplace_back(std::move(control));
        context.JinjaControls.push_back(emplacedControlPtr);
    }
}

void CodeGenBlobCacheControls(TCodeGenContext& context) {
    TImmediateControlsClass blobCacheControls {
        .Name = "BlobCache",
        .ClassName = "TBlobCacheControls",
    };
    for (const auto& field: {
        "MaxCacheDataSize",
        "MaxInFlightDataSize"
    }) {
        auto fieldPath = TStringBuilder() << blobCacheControls.Name << "." << field;
        TImmediateControl control {
            .Name = field,
            .FullPath = fieldPath,
            .HtmlName = fieldPath,
            .IsConfigBasedControl = false
        };
        auto emplacedControlPtr = &context.Controls.emplace_back(std::move(control));
        context.JinjaControls.push_back(emplacedControlPtr);
        blobCacheControls.Controls.push_back(emplacedControlPtr);
    }
    auto* res = &context.ControlClasses.emplace_back(std::move(blobCacheControls));
    context.JinjaControlClasses.push_back(res);
    context.JinjaRootControlClasses.push_back(res);
}

void CodeGenBlobStorageNonConfigControls(TCodeGenContext& context) {
    TImmediateControlsClass blobStorageControls {
        .Name = "BlobStorage",
        .ClassName = "TBlobStorageNonConfigControls",
    };
    for (const auto& field: {
        "EnablePutBatching",
        "EnableVPatch"
    }) {
        TImmediateControl control {
            .Name = field,
            .FullPath = TStringBuilder() << blobStorageControls.Name << "." << field,
            .HtmlName = TStringBuilder() << blobStorageControls.Name << "_" << field,
            .IsConfigBasedControl = false
        };
        auto emplacedControlPtr = &context.Controls.emplace_back(std::move(control));
        context.JinjaControls.push_back(emplacedControlPtr);
        blobStorageControls.Controls.push_back(emplacedControlPtr);
    }
    auto* res = &context.ControlClasses.emplace_back(std::move(blobStorageControls));
    context.JinjaControlClasses.push_back(res);
    context.JinjaRootControlClasses.push_back(res);
}

int main(int argc, char** argv) {
    if (argc < 3) {
        Cerr << "Usage: " << argv[0] << " INPUT OUTPUT ..." << Endl;
        return 1;
    }

    TCodeGenContext codeGenContext;


    const auto* d = NKikimrConfig::TImmediateControlsConfig::descriptor();
    for (int fieldIndex = 0; fieldIndex < d->field_count(); ++fieldIndex) {
        const auto* protoField = d->field(fieldIndex);
        if (protoField->type() != ::google::protobuf::FieldDescriptor::TYPE_MESSAGE) {
            continue;
        }
        CodeGenClass(*protoField, TVector<TString>{}, codeGenContext, true);
    }
    CodeGenBlobCacheControls(codeGenContext);
    CodeGenBlobStorageNonConfigControls(codeGenContext);
    CodeGenControlsForTablets(codeGenContext);


    jinja2::TemplateEnv env;
    env.AddGlobal("generator", jinja2::Reflect(std::string(__SOURCE_FILE__)));
    env.AddGlobal("control_classes", jinja2::Reflect(codeGenContext.JinjaControlClasses));
    env.AddGlobal("root_control_classes", jinja2::Reflect(codeGenContext.JinjaRootControlClasses));
    env.AddGlobal("controls", jinja2::Reflect(codeGenContext.JinjaControls));

    for (int i = 1; i < argc; i += 2) {
        if (!(i + 1 < argc)) {
            Cerr << "ERROR: missing output for " << argv[i] << Endl;
            return 1;
        }

        jinja2::Template t(&env);
        auto loaded = t.Load(TFileInput(argv[i]).ReadAll(), argv[i]);
        if (!loaded) {
            Cerr << "ERROR: " << loaded.error().ToString() << Endl;
            return 1;
        }

        auto rendered = t.RenderAsString({});
        if (!rendered) {
            Cerr << "ERROR: " << rendered.error().ToString() << Endl;
            return 1;
        }

        TFileOutput(argv[i + 1]).Write(rendered.value());
        Cout << "Generated " << argv[i + 1] << " from " << argv[i] << Endl;
    }

    return 0;
}
