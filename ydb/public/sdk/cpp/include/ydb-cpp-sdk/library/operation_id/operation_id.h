#pragma once

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace NKikimr {
namespace NOperationId {

class TOperationId {
    static constexpr int kEKindMinValue = 0;
    static constexpr int kEKindMaxValue = 10;
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
    };

    struct TData {
        std::string Key;
        std::string Value;
    };

    using TDataList = std::vector<std::unique_ptr<TData>>;

    TOperationId();
    explicit TOperationId(const std::string& string, bool allowEmpty = false);
    
    TOperationId(const TOperationId& other);
    TOperationId(TOperationId&& other) = default;

    TOperationId& operator=(const TOperationId& other);
    TOperationId& operator=(TOperationId&& other) = default;

    ~TOperationId() = default;

    EKind GetKind() const;
    void SetKind(const EKind& kind);

    const TDataList& GetData() const;
    TDataList& GetMutableData();

    const std::vector<const std::string*>& GetValue(const std::string& key) const;
    std::string GetSubKind() const;
    std::string ToString() const;

private:
    bool IsValidKind(int kind);
    void CopyData(const TOperationId::TDataList& otherData);

    EKind Kind;
    TDataList Data;
    std::unordered_map<std::string, std::vector<const std::string*>> Index;
};

void AddOptionalValue(TOperationId& operarionId, const std::string& key, const std::string& value);
void AddOptionalValue(TOperationId& operarionId, const std::string& key, const char* value, size_t size);
TOperationId::EKind ParseKind(const std::string_view value);

std::string FormatPreparedQueryIdCompat(const std::string& str);
bool DecodePreparedQueryIdCompat(const std::string& in, std::string& out);

} // namespace NOperationId
} // namespace NKikimr
