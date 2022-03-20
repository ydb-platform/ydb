#include "data_query.h"

#include <openssl/sha.h>

namespace NYdb {
namespace NTable {

TString GetQueryHash(const TString& text) {
    SHA256_CTX sha;
    SHA256_Init(&sha);
    SHA256_Update(&sha, text.data(), text.size());
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_Final(hash, &sha);
    return TString(reinterpret_cast<char*>(hash), sizeof(hash));
}

TString EncodeQuery(const TString& text, bool reversible) {
    if (reversible) {
        //TODO: may be compress and decompress this query
        return text;
    } else {
        return GetQueryHash(text);
    }
}

////////////////////////////////////////////////////////////////////////////////

TDataQuery::TImpl::TImpl(const TSession& session, const TString& text, bool keepText, const TString& id, bool allowMigration)
    : Session_(session)
    , Id_(id)
    , TextHash_(EncodeQuery(text, allowMigration))
    , Text_(keepText ? text : TMaybe<TString>())
{}

TDataQuery::TImpl::TImpl(const TSession& session, const TString& text, bool keepText, const TString& id, bool allowMigration,
    const ::google::protobuf::Map<TString, Ydb::Type>& types)
    : Session_(session)
    , Id_(id)
    , ParameterTypes_(types)
    , TextHash_(EncodeQuery(text, allowMigration))
    , Text_(keepText ? text : TMaybe<TString>())
{}

const TString& TDataQuery::TImpl::GetId() const {
    return Id_;
}

const ::google::protobuf::Map<TString, Ydb::Type>& TDataQuery::TImpl::GetParameterTypes() const {
    return ParameterTypes_;
}

const TString& TDataQuery::TImpl::GetTextHash() const {
    return TextHash_;
}

const TMaybe<TString>& TDataQuery::TImpl::GetText() const {
    return Text_;
}

} // namespace NTable
} // namespace NYdb
