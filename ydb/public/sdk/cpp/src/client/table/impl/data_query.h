#pragma once

#include <ydb-cpp-sdk/type_switcher.h>
#include <ydb-cpp-sdk/client/table/table.h>

#include <ydb/public/api/protos/ydb_table.pb.h>

#include <string>

namespace NYdb::inline Dev {
namespace NTable {

std::string EncodeQuery(const std::string& text, bool reversible);

////////////////////////////////////////////////////////////////////////////////

class TDataQuery::TImpl {
    friend class TDataQuery;

public:
    TImpl(const TSession& session, const std::string& text, bool keepText, const std::string& id, bool allowMigration);

    TImpl(const TSession& session, const std::string& text, bool keepText, const std::string& id, bool allowMigration,
        const ::google::protobuf::Map<TStringType, Ydb::Type>& types);

    const std::string& GetId() const;
    const ::google::protobuf::Map<TStringType, Ydb::Type>& GetParameterTypes() const;
    const std::string& GetTextHash() const;
    const std::optional<std::string>& GetText() const;

private:
    NTable::TSession Session_;
    std::string Id_;
    ::google::protobuf::Map<TStringType, Ydb::Type> ParameterTypes_;
    std::string TextHash_;
    std::optional<std::string> Text_;
};

} // namespace NTable
} // namespace NYdb
