#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NYdb {
namespace NTable {

TString EncodeQuery(const TString& text, bool reversible);

////////////////////////////////////////////////////////////////////////////////

class TDataQuery::TImpl {
    friend class TDataQuery;

public:
    TImpl(const TSession& session, const TString& text, bool keepText, const TString& id, bool allowMigration);

    TImpl(const TSession& session, const TString& text, bool keepText, const TString& id, bool allowMigration,
        const ::google::protobuf::Map<TString, Ydb::Type>& types);

    const TString& GetId() const;
    const ::google::protobuf::Map<TString, Ydb::Type>& GetParameterTypes() const;
    const TString& GetTextHash() const;
    const TMaybe<TString>& GetText() const;

private:
    NTable::TSession Session_;
    TString Id_;
    ::google::protobuf::Map<TString, Ydb::Type> ParameterTypes_;
    TString TextHash_;
    TMaybe<TString> Text_;
};

} // namespace NTable
} // namespace NYdb
