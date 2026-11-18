#include "query_id.h"
#include "helpers.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/yexception.h>
#include <util/string/escape.h>

#include <memory>

namespace NKikimr::NKqp {

namespace {

// Types are compared by their serialized bytes. Equal bytes always mean equal
// messages. Equal messages give equal bytes within one binary: Ydb::Type has no
// map fields, and known fields are written in a fixed order however the client
// sent them. The exception is unknown fields: they are written in the order they
// arrived, so the same unknown fields in another order make another key.
void AppendParameterTypes(TString& key, const TKqpQueryId::TParameterTypes& types) {
    google::protobuf::io::StringOutputStream stream(&key);
    google::protobuf::io::CodedOutputStream out(&stream);
    out.SetSerializationDeterministic(true);

    out.WriteVarint32(types.size());
    for (const auto& [name, type] : types) {
        out.WriteVarint32(name.size());
        out.WriteString(name);
        out.WriteVarint32(type.ByteSizeLong());
        type.SerializeWithCachedSizes(&out);
    }
}

} // anonymous namespace

// TGUCSettings::SerializeToString() writes its hash maps in iteration order,
// so equal settings may give different strings. Sorted keys make the bytes
// depend on the contents only.
TString TKqpQueryId::EncodeGUCSettings(const TGUCSettings& gUCSettings) {
    NJson::TJsonValue json;
    gUCSettings.ExportToJson(json);
    return NJson::WriteJson(json, /* formatOutput */ false, /* sortkeys */ true);
}

TKqpQueryId::TKqpQueryId(TStringBuf cluster, TStringBuf database, TStringBuf databaseId, TStringBuf userSid,
    const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
    TStringBuf encodedGUCSettings)
    : Text_(text)
    , Settings_(settings)
    , QueryParameterTypes_(std::move(queryParameterTypes))
{
    switch (Settings_.QueryType) {
        case NKikimrKqp::QUERY_TYPE_SQL_DML:
        case NKikimrKqp::QUERY_TYPE_SQL_SCAN:
        case NKikimrKqp::QUERY_TYPE_AST_DML:
        case NKikimrKqp::QUERY_TYPE_AST_SCAN:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY:
        case NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT:
            break;

        default:
            Y_ENSURE(false, "Unsupported request type");
    }

    const size_t headerSize = FieldCount * sizeof(ui32);
    // Only a guess to avoid reallocations, not part of the format: 64 bytes for
    // the settings and the parameter count, 24 bytes per parameter name and type.
    const size_t sizeEstimate = headerSize + cluster.size() + database.size() + databaseId.size() + userSid.size()
        + text.size() + encodedGUCSettings.size() + 64 + (QueryParameterTypes_ ? QueryParameterTypes_->size() * 24 : 0);
    TString key;
    key.reserve(sizeEstimate);
    key.resize(headerSize);

    ui32 ends[FieldCount];
    size_t nextField = 0;
    auto endField = [&](EField field) {
        Y_DEBUG_ABORT_UNLESS(field == nextField++, "fields must be written in EField order");
        ends[field] = key.size();
    };
    key.append(cluster);
    endField(FieldCluster);
    key.append(database);
    endField(FieldDatabase);
    key.append(databaseId);
    endField(FieldDatabaseId);
    key.append(userSid);
    endField(FieldUserSid);
    key.append(text);
    endField(FieldText);
    Settings_.AppendCacheKey(key);
    endField(FieldSettings);
    key.append(encodedGUCSettings);
    endField(FieldGUCSettings);
    Y_DEBUG_ABORT_UNLESS(nextField == FieldCount);
    memcpy(key.begin(), ends, headerSize);

    // An absent map differs from an empty one.
    key.push_back(QueryParameterTypes_ != nullptr);
    if (QueryParameterTypes_) {
        AppendParameterTypes(key, *QueryParameterTypes_);
    }

    Key_ = std::move(key);
    Hash_ = THash<TString>()(Key_);
}

TKqpQueryId::TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId, const TString& userSid,
    const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
    const TGUCSettings& gUCSettings)
    : TKqpQueryId(cluster, database, databaseId, userSid, text, settings, std::move(queryParameterTypes),
        EncodeGUCSettings(gUCSettings))
{}

TKqpQueryId::TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId,
    const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
    const TGUCSettings& gUCSettings)
    : TKqpQueryId(cluster, database, databaseId, "", text, settings, std::move(queryParameterTypes), gUCSettings)
{}

TStringBuf TKqpEncodedGUCSettings::Get(const TGUCSettings& gUCSettings) {
    if (!Source_ || !(*Source_ == gUCSettings)) {
        Source_ = gUCSettings;
        Encoded_ = TKqpQueryId::EncodeGUCSettings(gUCSettings);
    }
    return Encoded_;
}

TStringBuf TKqpQueryId::Field(EField field) const {
    ui32 ends[FieldCount];
    memcpy(ends, Key_.data(), sizeof(ends));
    const ui32 begin = field == 0 ? sizeof(ends) : ends[field - 1];
    return TStringBuf(Key_.data() + begin, ends[field] - begin);
}

void TKqpQueryId::SetUserSid(TStringBuf userSid) {
    if (userSid != UserSid()) {
        *this = TKqpQueryId(Cluster(), Database(), DatabaseId(), userSid, Text_, Settings_, QueryParameterTypes_,
            Field(FieldGUCSettings));
    }
}

void TKqpQueryId::SetQueryParameterTypes(std::shared_ptr<const TParameterTypes> queryParameterTypes) {
    *this = TKqpQueryId(Cluster(), Database(), DatabaseId(), UserSid(), Text_, Settings_, std::move(queryParameterTypes),
        Field(FieldGUCSettings));
}

bool TKqpQueryId::IsSql() const {
    return IsSqlQuery(Settings_.QueryType);
}

bool TKqpQueryId::operator==(const TKqpQueryId& other) const {
    // Hash_ covers the whole Key_: keys with the same long text and different
    // parameter types are told apart without walking the text.
    return Hash_ == other.Hash_ && Key_ == other.Key_;
}

TString TKqpQueryId::SerializeToString() const {
    TStringBuilder result = TStringBuilder() << "{"
        << "Cluster: " << Cluster() << ", "
        << "Database: " << Database() << ", "
        << "DatabaseId: " << DatabaseId() << ", "
        << "UserSid: " << UserSid() << ", "
        << "Text: " << EscapeC(Text_) << ", "
        << "Settings: " << Settings_.SerializeToString() << ", ";
    if (QueryParameterTypes_) {
        result << "QueryParameterTypes: [";
        for (const auto& param : *QueryParameterTypes_) {
            result << "name: " << param.first << ", type: " << param.second.ShortDebugString();
        }
        result << "], ";
    } else {
        result << "QueryParameterTypes: <empty>, ";
    }

    result << "GUCSettings: " << EscapeC(Field(FieldGUCSettings)) << "}";
    return result;
}

} // namespace NKikimr::NKqp
