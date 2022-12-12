#pragma once

#include <ydb/core/protos/config.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>

#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace NKikimr::NConsole {

// Base class for config validators. Validators are used
// by CMS to check uploaded cluster configs.
//
// All validators should provide CheckConfig method
// implementation. To become active validator should be
// registered using RegisterValidator method.
//
// CheckConfig implementation should be thread-safe to
// allow simultaneous validators usage from different
// actors.
class IConfigValidator : public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IConfigValidator>;

    // Config item kinds set specifies which config items are checked by
    // validator. Modifications in other items will not trigger validator.
    // Empty set means all items are checked.
    IConfigValidator(const TString &name,
                     THashSet<ui32> itemKinds)
        : Name(name)
        , CheckedConfigItemKinds(std::move(itemKinds))
        , Enabled(true)
    {
    }

    IConfigValidator(const TString &name,
                     ui32 itemKind)
        : IConfigValidator(name, THashSet<ui32>({itemKind}))
    {
    }

    const TString &GetName() const
    {
        return Name;
    }

    const THashSet<ui32> GetCheckedConfigItemKinds() const
    {
        return CheckedConfigItemKinds;
    }

    void Enable()
    {
        Enabled = true;
    }

    void Disable()
    {
        Enabled = false;
    }

    bool IsEnabled() const
    {
        return Enabled;
    }

    virtual TString GetDescription() const
    {
        return GetName() + " configs validator";
    }

    // Return true if new config is OK, and false otherwise. Output issues
    // vector should be filled with appropriate error message(s) if false
    // is returned. Info and warning messages may be added to output
    // issues for both correct and incorrect configs.
    virtual bool CheckConfig(const NKikimrConfig::TAppConfig &oldConfig,
                             const NKikimrConfig::TAppConfig &newConfig,
                             TVector<Ydb::Issue::IssueMessage> &issues) const = 0;

protected:
    void AddIssue(TVector<Ydb::Issue::IssueMessage> &issues,
                  const TString &msg,
                  ui32 severity = NYql::TSeverityIds::S_ERROR) const
    {
        Ydb::Issue::IssueMessage issue;
        issue.set_message(Sprintf("%s validator: %s", Name.data(), msg.data()));
        issue.set_severity(severity);
        issues.push_back(issue);
    }

    void AddError(TVector<Ydb::Issue::IssueMessage> &issues,
                  const TString &msg) const
    {
        AddIssue(issues, msg, NYql::TSeverityIds::S_ERROR);
    }

    void AddWarning(TVector<Ydb::Issue::IssueMessage> &issues,
                    const TString &msg) const
    {
        AddIssue(issues, msg, NYql::TSeverityIds::S_WARNING);
    }

    void AddInfo(TVector<Ydb::Issue::IssueMessage> &issues,
                 const TString &msg) const
    {
        AddIssue(issues, msg, NYql::TSeverityIds::S_INFO);
    }

private:
    TString Name;
    THashSet<ui32> CheckedConfigItemKinds;
    bool Enabled;
};

// All validators should be registered before Console activation.
// An attempt to register validator after Console activation will
// cause a failure.
// All registered validators should have unique names.
void RegisterValidator(IConfigValidator::TPtr validator);

} // namespace NKikimr::NConsole
