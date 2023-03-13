#pragma once

#include <ydb/public/lib/operation_id/protos/operation_id.pb.h>

#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr {
namespace NOperationId {

class TOperationId : public Ydb::TOperationId {
public:
    TOperationId();
    explicit TOperationId(const TString& string, bool allowEmpty = false);
    const TVector<const TString*>& GetValue(const TString& key) const;
    TString GetSubKind() const;

private:
     THashMap<TString, TVector<const TString*>> Index_;
};

TString ProtoToString(const Ydb::TOperationId& proto);
void AddOptionalValue(Ydb::TOperationId& proto, const TString& key, const TString& value);
void AddOptionalValue(Ydb::TOperationId& proto, const TString& key, const char* value, size_t size);
Ydb::TOperationId::EKind ParseKind(const TStringBuf value);

TString FormatPreparedQueryIdCompat(const TString& str);
bool DecodePreparedQueryIdCompat(const TString& in, TString& out);

} // namespace NOperationId
} // namespace NKikimr
