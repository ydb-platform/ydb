#pragma once

#include <ydb/library/yql/public/issue/protos/issue_severity.pb.h>

#include <library/cpp/resource/resource.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/message.h>

#include <util/generic/hash.h>
#include <util/generic/singleton.h>
#include <util/generic/yexception.h>
#include <util/string/subst.h>

#ifdef _win_
#ifdef GetMessage
#undef GetMessage
#endif
#endif

namespace NYql {

using TIssueCode = ui32;
using ESeverity = NYql::TSeverityIds::ESeverityId;
const TIssueCode DEFAULT_ERROR = 0;
const TIssueCode UNEXPECTED_ERROR = 1;

inline TString SeverityToString(ESeverity severity)
{
    auto ret = NYql::TSeverityIds::ESeverityId_Name(severity);
    return ret.empty() ? "Unknown" : to_title(ret.substr(2)); //remove prefix "S_"
}

template <typename T>
inline TString IssueCodeToString(TIssueCode id) {
    auto ret = T::EIssueCode_Name(static_cast<typename T::EIssueCode>(id));
    if (!ret.empty()) {
        SubstGlobal(ret, '_', ' ');
        return to_title(ret);
    } else {
        return "Unknown";
    }
}

template<typename TProto, const char* ResourceName>
class TIssueId {
    TProto ProtoIssues_;
    THashMap<TIssueCode, NYql::TSeverityIds::ESeverityId> IssuesMap_;
    THashMap<TIssueCode, TString> IssuesFormatMap_;

    const google::protobuf::Descriptor* GetProtoDescriptor() const {
        auto ret = ProtoIssues_.GetDescriptor();
        Y_ENSURE(ret != nullptr, "Bad proto file");
        return ret;
    }

    bool CheckSeverityNameFormat(const TString& name) const {
        if (name.size() > 2 && name.substr(0,2) == "S_") {
            return true;
        }
        return false;
    }

public:
    ESeverity GetSeverity(TIssueCode id) const {
        auto it = IssuesMap_.find(id);
        Y_ENSURE(it != IssuesMap_.end(), "Unknown issue id: "
            << id << "(" << IssueCodeToString<TProto>(id) << ")");
        return it->second;
    }

    TString GetMessage(TIssueCode id) const {
        auto it = IssuesFormatMap_.find(id);
        Y_ENSURE(it != IssuesFormatMap_.end(), "Unknown issue id: "
            << id << "(" << IssueCodeToString<TProto>(id) << ")");
        return it->second;
    }

    TIssueId() {
        auto configData = NResource::Find(TStringBuf(ResourceName));
        if (!::google::protobuf::TextFormat::ParseFromString(configData, &ProtoIssues_)) {
            Y_ENSURE(false, "Bad format of protobuf data file, resource: " << ResourceName);
        }

        auto sDesc = TSeverityIds::ESeverityId_descriptor();
        for (int i = 0; i < sDesc->value_count(); i++) {
            const auto& name = sDesc->value(i)->name();
            Y_ENSURE(CheckSeverityNameFormat(name),
                "Wrong severity name: " << name << ". Severity must starts with \"S_\" prefix");
        }

        for (const auto& x : ProtoIssues_.ids()) {
            auto rv = IssuesMap_.insert(std::make_pair(x.code(), x.severity()));
            Y_ENSURE(rv.second, "Duplicate issue code found, code: "
                << static_cast<int>(x.code())
                << "(" << IssueCodeToString<TProto>(x.code()) <<")");
        }

        // Check all IssueCodes have mapping to severity
        auto eDesc = TProto::EIssueCode_descriptor();
        for (int i = 0; i < eDesc->value_count(); i++) {
            auto it = IssuesMap_.find(eDesc->value(i)->number());
            Y_ENSURE(it != IssuesMap_.end(), "IssueCode: "
                << eDesc->value(i)->name()
                << " is not found in protobuf data file");
        }

        for (const auto& x : ProtoIssues_.ids()) {
            auto rv = IssuesFormatMap_.insert(std::make_pair(x.code(), x.format()));
            Y_ENSURE(rv.second, "Duplicate issue code found, code: "
                << static_cast<int>(x.code())
                << "(" << IssueCodeToString<TProto>(x.code()) <<")");
        }
    }
};

template<typename TProto, const char* ResourceName>
inline ESeverity GetSeverity(TIssueCode id) {
    return Singleton<TIssueId<TProto, ResourceName>>()->GetSeverity(id);
}

template<typename TProto, const char* ResourceName>
inline TString GetMessage(TIssueCode id) {
    return Singleton<TIssueId<TProto, ResourceName>>()->GetMessage(id);
}

}
