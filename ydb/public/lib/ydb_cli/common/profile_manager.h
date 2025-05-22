#pragma once
#include <library/cpp/yaml/as/tstring.h>

namespace NYdb {
namespace NConsoleClient {

class IProfile {
public:
    virtual ~IProfile() = default;

    virtual TString GetName() const = 0;
    virtual bool IsEmpty() const = 0;
    virtual bool Has(const TString& key) const noexcept = 0;
    virtual void SetValue(const TString& key, const YAML::Node& value) = 0;
    virtual void SetValue(const TString& key, const TString& value) = 0;
    virtual const YAML::Node GetValue(const TString& key) const = 0;
    virtual void RemoveValue(const TString& key) = 0;
};

class IProfileManager : public std::enable_shared_from_this<IProfileManager> {
public:
    virtual ~IProfileManager() = default;

    virtual bool HasProfile(const TString& profileName) const = 0;
    // Get existing profile or create new
    virtual std::shared_ptr<IProfile> CreateProfile(const TString& profileName) = 0;
    virtual std::shared_ptr<IProfile> GetProfile(const TString& profileName) = 0;
    virtual TVector<TString> ListProfiles() const = 0;
    virtual bool RemoveProfile(const TString& profileName) = 0;
    virtual void SetActiveProfile(const TString& profileName) = 0;
    virtual void DeactivateProfile() = 0;
    virtual TString GetActiveProfileName() const = 0;
    virtual std::shared_ptr<IProfile> GetActiveProfile() = 0;
};

std::shared_ptr<IProfileManager> CreateProfileManager(const TString& configPath);

}
}
