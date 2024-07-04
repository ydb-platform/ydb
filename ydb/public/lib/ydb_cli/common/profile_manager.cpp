#include "profile_manager.h"

#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/env.h>

namespace NYdb {
namespace NConsoleClient {

class TProfile : public IProfile {
public:
    TProfile(const TString& name, YAML::Node yamlProfile, bool& profilesChanged, std::shared_ptr<IProfileManager> manager)
        : Name(name)
        , YamlProfile(yamlProfile)
        , ProfilesChanged(profilesChanged)
        , ProfileManager(manager)
    {
    }

    TString GetName() const override {
        return Name;
    }

    bool IsEmpty() const override {
        return YamlProfile.size() == 0;
    }

    bool Has(const TString& key) const noexcept override {
        return static_cast<bool>(YamlProfile[key]);
    }

    void SetValue(const TString& key, const YAML::Node& value) override {
        ProfilesChanged = true;
        YamlProfile[key] = value;
    }

    void SetValue(const TString& key, const TString& value) override {
        ProfilesChanged = true;
        YamlProfile[key] = value;
    }

    const YAML::Node GetValue(const TString& key) const override {
        return YamlProfile[key];
    }

    void RemoveValue(const TString& key) override {
        ProfilesChanged = true;
        YamlProfile.remove(key);
    }

private:
    TString Name;
    YAML::Node YamlProfile;
    bool& ProfilesChanged;
    std::shared_ptr<IProfileManager> ProfileManager;
};

class TProfileManager : public IProfileManager {
public:
    TProfileManager(const TString& configPath)
        : ConfigPath(configPath)
    {
        LoadConfig();
    }

    ~TProfileManager() {
        if (ProfilesChanged) {
            SaveConfig();
        }
    }

    bool HasProfile(const TString& profileName) const override {
        return Config["profiles"] && Config["profiles"][profileName];
    }

    std::shared_ptr<IProfile> CreateProfile(const TString& profileName) override {
        ProfilesChanged = true;
        Config["profiles"][profileName] = YAML::Node();
        return GetProfile(profileName);
    }

    std::shared_ptr<IProfile> GetProfile(const TString& profileName) override {
        return std::make_shared<TProfile>(profileName, Config["profiles"][profileName], ProfilesChanged, shared_from_this());
    }

    TVector<TString> ListProfiles() const override {
        const auto profilesNode = Config["profiles"];
        TVector<TString> result;
        if (Config["profiles"]) {
            result.reserve(Config["profiles"].size());
            for (const auto& profile : Config["profiles"]) {
                result.push_back(profile.first.as<TString>());
            }
        }
        return result;
    }

    bool RemoveProfile(const TString& profileName) override {
        if (Config["profiles"] && Config["profiles"][profileName]) {
            if (GetActiveProfileName() == profileName) {
                DeactivateProfile();
            }
            Config["profiles"].remove(profileName);
            ProfilesChanged = true;
            return true;
        }
        return false;
    }

    void SetActiveProfile(const TString& profileName) override {
        Config["active_profile"] = profileName;
        ProfilesChanged = true;
    }

    void DeactivateProfile() override {
        if (Config["active_profile"]) {
            Config.remove("active_profile");
            ProfilesChanged = true;
        }
    }

    TString GetActiveProfileName() const override {
        if (Config["active_profile"]) {
            return Config["active_profile"].as<TString>();
        } else {
            return TString();
        }
    }

    std::shared_ptr<IProfile> GetActiveProfile() override {
        const TString activeProfileName = GetActiveProfileName();
        if (activeProfileName && Config["profiles"][activeProfileName]) {
            return std::make_shared<TProfile>(
                activeProfileName, Config["profiles"][activeProfileName], ProfilesChanged, shared_from_this());
        } else {
            return nullptr;
        }
    }

private:
    void LoadConfig() {
        TFsPath configFilePath(ConfigPath);
        configFilePath.Fix();
        try {
            if (configFilePath.Exists()) {
                Config = YAML::LoadFile(configFilePath.GetPath());
                return;
            }
        }
        catch (const std::exception& e) {
            Cerr << "(!) Couldn't load profiles from config file \"" << configFilePath.GetPath()
                << "\". " << e.what() << Endl;
        }
    }

    void SaveConfig() {
        TFsPath configFilePath(ConfigPath);
        configFilePath.Fix();
        try {
            if (!configFilePath.Parent().Exists()) {
                configFilePath.Parent().MkDirs();
            }
            if (TFileStat(configFilePath).Mode & (S_IRGRP | S_IROTH)) {
                int chmodResult = Chmod(configFilePath.GetPath().c_str(), S_IRUSR | S_IWUSR);
                if (chmodResult) {
                    Cerr << "Couldn't change permissions for the file \"" << configFilePath.GetPath() << "\"" << Endl;
                    exit(chmodResult);
                }
            }
            TFileOutput resultConfigFile(TFile(configFilePath, CreateAlways | WrOnly | AWUser | ARUser));
            resultConfigFile << YAML::Dump(Config);
        }
        catch (const std::exception& e) {
            Cerr << "(!) Couldn't save profiles to config file \"" << configFilePath.GetPath()
                << "\". " << e.what() << Endl;
        }
    }

    TString ConfigPath;
    YAML::Node Config;
    bool ProfilesChanged = false;
};

std::shared_ptr<IProfileManager> CreateProfileManager(const TString& configPath) {
    return std::make_shared<TProfileManager>(configPath);
}

}
}
