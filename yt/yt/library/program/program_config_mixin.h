#pragma once

#include "program.h"

#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <library/cpp/yt/string/enum.h>

#include <util/stream/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TConfig, class TDynamicConfig = void>
class TProgramConfigMixin
{
protected:
    explicit TProgramConfigMixin(
        NLastGetopt::TOpts& opts,
        bool required = true,
        const TString& argumentName = "config")
        : ArgumentName_(argumentName)
    {
        auto opt = opts
            .AddLongOption(TString(argumentName), Format("path to %v file", argumentName))
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
                Format("print %v schema and exit", argumentName))
            .OptionalValue(YsonSchemaFormat_, "FORMAT")
            .StoreResult(&ConfigSchema_);
        opts
            .AddLongOption(
                Format("%v-template", argumentName),
                Format("print %v template and exit", argumentName))
            .SetFlag(&ConfigTemplate_);
        opts
            .AddLongOption(
                Format("%v-actual", argumentName),
                Format("print actual %v and exit", argumentName))
            .SetFlag(&ConfigActual_);
        opts
            .AddLongOption(
                Format("%v-unrecognized-strategy", argumentName),
                Format("configure strategy for unrecognized attributes in %v", argumentName))
            .Handler1T<TStringBuf>([this](TStringBuf value) {
                UnrecognizedStrategy_ = ParseEnum<NYTree::EUnrecognizedStrategy>(value);
            });

        if constexpr (std::is_same_v<TDynamicConfig, void>) {
            return;
        }

        opts
            .AddLongOption(
                Format("dynamic-%v-schema", argumentName),
                Format("print %v schema and exit", argumentName))
            .OptionalValue(YsonSchemaFormat_, "FORMAT")
            .StoreResult(&DynamicConfigSchema_);
        opts
            .AddLongOption(
                Format("dynamic-%v-template", argumentName),
                Format("print dynamic %v template and exit", argumentName))
            .SetFlag(&DynamicConfigTemplate_);
    }

    TIntrusivePtr<TConfig> GetConfig(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigPath_) {
            return nullptr;
        }

        if (!Config_) {
            LoadConfig();
        }
        return Config_;
    }

    NYTree::INodePtr GetConfigNode(bool returnNullIfNotSupplied = false)
    {
        if (returnNullIfNotSupplied && !ConfigPath_) {
            return nullptr;
        }

        if (!ConfigNode_) {
            LoadConfigNode();
        }
        return ConfigNode_;
    }

    bool HandleConfigOptions()
    {
        auto print = [] (const auto& config) {
            using namespace NYson;
            TYsonWriter writer(&Cout, EYsonFormat::Pretty);
            config->Save(&writer);
            Cout << Flush;
        };
        auto printSchema = [] (const auto& config, TString format) {
            if (format == YsonSchemaFormat_) {
                using namespace NYson;
                TYsonWriter writer(&Cout, EYsonFormat::Pretty);
                config->WriteSchema(&writer);
                Cout << Endl;
            } else {
                THROW_ERROR_EXCEPTION("Unknown schema format %v", format);
            }
        };
        if (!ConfigSchema_.empty()) {
            printSchema(New<TConfig>(), ConfigSchema_);
            return true;
        }
        if (ConfigTemplate_) {
            print(New<TConfig>());
            return true;
        }
        if (ConfigActual_) {
            print(GetConfig());
            return true;
        }

        if constexpr (!std::is_same_v<TDynamicConfig, void>) {
            if (!DynamicConfigSchema_.empty()) {
                printSchema(New<TDynamicConfig>(), DynamicConfigSchema_);
                return true;
            }
            if (DynamicConfigTemplate_) {
                print(New<TDynamicConfig>());
                return true;
            }
        }
        return false;
    }

private:
    void LoadConfigNode()
    {
        using namespace NYTree;

        if (!ConfigPath_){
            THROW_ERROR_EXCEPTION("Missing --%v option", ArgumentName_);
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

    const TString ArgumentName_;

    TString ConfigPath_;
    TString ConfigSchema_;
    bool ConfigTemplate_;
    bool ConfigActual_;
    TString DynamicConfigSchema_;
    bool DynamicConfigTemplate_ = false;
    NYTree::EUnrecognizedStrategy UnrecognizedStrategy_ = NYTree::EUnrecognizedStrategy::KeepRecursive;

    static constexpr auto YsonSchemaFormat_ = "yson-schema";

    TIntrusivePtr<TConfig> Config_;
    NYTree::INodePtr ConfigNode_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
