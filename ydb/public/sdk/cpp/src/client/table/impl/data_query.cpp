#include "data_query.h"

#include <openssl/sha.h>

namespace NYdb::inline Dev {
namespace NTable {

std::string GetQueryHash(const std::string& text) {
    SHA256_CTX sha;
    SHA256_Init(&sha);
    SHA256_Update(&sha, text.data(), text.size());
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &sha);
    return std::string(reinterpret_cast<char*>(hash), sizeof(hash));
}

std::string EncodeQuery(const std::string& text, bool reversible) {
    if (reversible) {
        //TODO: may be compress and decompress this query
        return text;
    } else {
        return GetQueryHash(text);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDataQuery::TImpl::TImpl(const TSession& session, const std::string& text, bool keepText, const std::string& id, bool allowMigration)
    : Session_(session)
    , Id_(id)
    , TextHash_(EncodeQuery(text, allowMigration))
    , Text_(keepText ? text : std::optional<std::string>())
{}

TDataQuery::TImpl::TImpl(const TSession& session, const std::string& text, bool keepText, const std::string& id, bool allowMigration,
    const ::google::protobuf::Map<TStringType, Ydb::Type>& types)
    : Session_(session)
    , Id_(id)
    , ParameterTypes_(types)
    , TextHash_(EncodeQuery(text, allowMigration))
    , Text_(keepText ? text : std::optional<std::string>())
{}

const std::string& TDataQuery::TImpl::GetId() const {
    return Id_;
}

const ::google::protobuf::Map<TStringType, Ydb::Type>& TDataQuery::TImpl::GetParameterTypes() const {
    return ParameterTypes_;
}

const std::string& TDataQuery::TImpl::GetTextHash() const {
    return TextHash_;
}

const std::optional<std::string>& TDataQuery::TImpl::GetText() const {
    return Text_;
}

} // namespace NTable
} // namespace NYdb
