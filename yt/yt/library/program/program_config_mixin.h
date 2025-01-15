#pragma once

#include "program_mixin.h"

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/string/enum.h>

#include <library/cpp/yt/system/exit.h>

#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig = void>
class TProgramConfigMixin
    : public virtual TProgramMixinBase
{
protected:
    explicit TProgramConfigMixin(
        NLastGetopt::TOpts& opts,
        bool required = true,
        const TString& argumentName = "config")
        : ArgumentName_(argumentName)
    {
        auto opt = opts
            .AddLongOption(TString(argumentName), Format("path to %v file (in YSON format)", argumentName))
            .Handler0([&] { ConfigFlag_ = true; })
            .StoreMappedResult(&ConfigPath_, &CheckPathExistsArgMapper)
            .RequiredArgument("FILE");
        if (required) {
            opt.Required();
        } else {
            opt.Optional();
        }

        opts
            .AddLongOption(
                Format("%v-schema", argumentName),
                Format("Prints %v schema and exit", argumentName))
            .OptionalValue(YsonSchemaFormat_, "FORMAT")
            .Handler0([&] { ConfigSchemaFlag_ = true; })
            .StoreResult(&ConfigSchema_);
        opts
            .AddLongOption(
                Format("%v-template", argumentName),
                Format("Prints %v template and exit", argumentName))
            .OptionalArgument()
            .SetFlag(&ConfigTemplateFlag_);
        opts
            .AddLongOption(
                Format("%v-actual", argumentName),
                Format("Prints actual %v and exit", argumentName))
            .OptionalArgument()
            .SetFlag(&ConfigActualFlag_);
        opts
            .AddLongOption(
                Format("%v-unrecognized", argumentName),
                Format("Prints unrecognized %v and exit", argumentName))
            .OptionalArgument()
            .SetFlag(&ConfigUnrecognizedFlag_);

        TStringBuilder unrecognizedStrategies;
        for (const auto& strategy: TEnumTraits<NYTree::EUnrecognizedStrategy>::GetDomainNames()) {
            if (unrecognizedStrategies.GetLength()) {
                unrecognizedStrategies.AppendString(", ");
            }
            unrecognizedStrategies.AppendString(CamelCaseToUnderscoreCase(strategy));
        }
        opts
            .AddLongOption(
                Format("%v-unrecognized-strategy", argumentName),
                Format("Configures strategy for unrecognized attributes in %v, variants: %v",
                    argumentName,
                    unrecognizedStrategies.Flush()))
            .DefaultValue(FormatEnum(UnrecognizedStrategy_))
            .Handler1T<TStringBuf>([&] (TStringBuf value) {
                UnrecognizedStrategy_ = ParseEnum<NYTree::EUnrecognizedStrategy>(value);
            });

        if constexpr (!std::is_same_v<TDynamicConfig, void>) {
            opts
                .AddLongOption(
                    Format("dynamic-%v-schema", argumentName),
                    Format("Prints %v schema", argumentName))
                .OptionalValue(YsonSchemaFormat_, "FORMAT")
                .Handler0([&] { DynamicConfigSchemaFlag_ = true; })
                .StoreResult(&DynamicConfigSchema_);
            opts
                .AddLongOption(
                    Format("dynamic-%v-template", argumentName),
                    Format("Prints dynamic %v template", argumentName))
                .OptionalArgument()
                .SetFlag(&DynamicConfigTemplateFlag_);
        }

        RegisterMixinCallback([&] { Handle(); });
    }

    TIntrusivePtr<TConfig> GetConfig(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigFlag_) {
            return nullptr;
        }

        if (!Config_) {
            LoadConfig();
        }
        return Config_;
    }

    NYTree::INodePtr GetConfigNode(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigFlag_) {
            return nullptr;
        }

        if (!ConfigNode_) {
            LoadConfigNode();
        }
        return ConfigNode_;
    }

private:
    const TString ArgumentName_;

    bool ConfigFlag_;
    TString ConfigPath_;
    bool ConfigSchemaFlag_ = false;
    TString ConfigSchema_;
    bool ConfigTemplateFlag_;
    bool ConfigActualFlag_;
    bool ConfigUnrecognizedFlag_;
    bool DynamicConfigSchemaFlag_ = false;
    TString DynamicConfigSchema_;
    bool DynamicConfigTemplateFlag_ = false;
    NYTree::EUnrecognizedStrategy UnrecognizedStrategy_ = NYTree::EUnrecognizedStrategy::KeepRecursive;

    static constexpr auto YsonSchemaFormat_ = "yson-schema";

    TIntrusivePtr<TConfig> Config_;
    NYTree::INodePtr ConfigNode_;

    void LoadConfigNode()
    {
        using namespace NYTree;

        if (!ConfigFlag_){
            THROW_ERROR_EXCEPTION("Missing %qv option", ArgumentName_);
        }

        try {
            TIFStream stream(ConfigPath_);
            ConfigNode_ = ConvertToNode(&stream);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error parsing %v file %v",
                ArgumentName_,
                ConfigPath_)
                << ex;
        }
    }

    void LoadConfig()
    {
        if (!ConfigNode_) {
            LoadConfigNode();
        }

        try {
            Config_ = New<TConfig>();
            Config_->SetUnrecognizedStrategy(UnrecognizedStrategy_);
            Config_->Load(ConfigNode_);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error loading %v file %v",
                ArgumentName_,
                ConfigPath_)
                << ex;
        }
    }

    void Handle()
    {
        auto print = [] (const auto& config) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            Cout << Endl;
        };

        auto printNode = [] (const auto& node) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            NYTree::Serialize(node, &writer);
            Cout << Endl;
        };

        auto printSchema = [] (const auto& config, TString format) {
            if (format == YsonSchemaFormat_) {
                using namespace NYson;
                TYsonWriter writer(&Cout, EYsonFormat::Pretty);
                config->WriteSchema(&writer);
                Cout << Endl;
            } else {
                THROW_ERROR_EXCEPTION("Unknown schema format %Qv", format);
            }
        };

        if (ConfigSchemaFlag_) {
            printSchema(New<TConfig>(), ConfigSchema_);
            Exit(EProcessExitCode::OK);
        }

        if (ConfigTemplateFlag_) {
            print(New<TConfig>());
            Exit(EProcessExitCode::OK);
        }

        if (ConfigActualFlag_) {
            print(GetConfig());
            Exit(EProcessExitCode::OK);
        }

        if (ConfigUnrecognizedFlag_) {
            auto unrecognized = GetConfig()->GetRecursiveUnrecognized();
            printNode(*unrecognized);
            Exit(EProcessExitCode::OK);
        }

        if constexpr (!std::is_same_v<TDynamicConfig, void>) {
            if (DynamicConfigSchemaFlag_) {
                printSchema(New<TDynamicConfig>(), DynamicConfigSchema_);
                Exit(EProcessExitCode::OK);
            }

            if (DynamicConfigTemplateFlag_) {
                print(New<TDynamicConfig>());
                Exit(EProcessExitCode::OK);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
