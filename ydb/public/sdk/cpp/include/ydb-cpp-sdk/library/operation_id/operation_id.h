#pragma once

#include <memory>
#include <string>
#include <vector>

namespace Ydb {
    class TOperationId;
}

namespace NKikimr {
inline namespace Dev {
namespace NOperationId {

class TOperationId {
public:
    enum EKind : int {
        UNUSED = 0,
        OPERATION_DDL = 1,
        OPERATION_DML = 2,
        SESSION_YQL = 3,
        PREPARED_QUERY_ID = 4,
        CMS_REQUEST = 5,
        EXPORT = 6,
        BUILD_INDEX = 7,
        IMPORT = 8,
        SCRIPT_EXECUTION = 9,
        SS_BG_TASKS = 10,
        INCREMENTAL_BACKUP = 11,
        RESTORE = 12,
    };

    struct TData {
        std::string Key;
        std::string Value;
    };

    TOperationId();
    explicit TOperationId(const std::string& string, bool allowEmpty = false);

    TOperationId(const TOperationId& other);
    TOperationId(TOperationId&& other);

    TOperationId& operator=(const TOperationId& other);
    TOperationId& operator=(TOperationId&& other);

    ~TOperationId();

    EKind GetKind() const;
    void SetKind(const EKind& kind);

    std::vector<TData> GetData() const;

    void AddOptionalValue(const std::string& key, const std::string& value);
    const std::vector<const std::string*>& GetValue(const std::string& key) const;

    std::string GetSubKind() const;
    std::string ToString() const;

    const Ydb::TOperationId& GetProto() const;
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;
};

std::string ProtoToString(const Ydb::TOperationId& proto);

void AddOptionalValue(Ydb::TOperationId& operarionId, const std::string& key, const std::string& value);

TOperationId::EKind ParseKind(const std::string_view value);

std::string FormatPreparedQueryIdCompat(const std::string& str);
bool DecodePreparedQueryIdCompat(const std::string& in, std::string& out);

}
}
}
