#include "query_id.h"
#include "helpers.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

#include <library/cpp/json/json_writer.h>

#include <util/digest/numeric.h>
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
TKqpEncodedGUC TKqpEncodedGUC::Encode(const TGUCSettings& gUCSettings) {
    NJson::TJsonValue json;
    gUCSettings.ExportToJson(json);
    return TKqpEncodedGUC(NJson::WriteJson(json, /* formatOutput */ false, /* sortkeys */ true));
}

TKqpQueryId::TKqpQueryId(TStringBuf cluster, TStringBuf database, TStringBuf databaseId, TStringBuf userSid,
    const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
    const TKqpEncodedGUC& encodedGUCSettings)
    : Text_(text)
    , TextHash_(THash<TString>()(text))
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

    BuildKey(cluster, database, databaseId, userSid, encodedGUCSettings.Bytes());
}

TKqpQueryId::TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId, const TString& userSid,
    const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
    const TGUCSettings& gUCSettings)
    : TKqpQueryId(cluster, database, databaseId, userSid, text, settings, std::move(queryParameterTypes),
        TKqpEncodedGUC::Encode(gUCSettings))
{}

TKqpQueryId::TKqpQueryId(const TString& cluster, const TString& database, const TString& databaseId,
    const TString& text, const TKqpQuerySettings& settings, std::shared_ptr<const TParameterTypes> queryParameterTypes,
    const TGUCSettings& gUCSettings)
    : TKqpQueryId(cluster, database, databaseId, "", text, settings, std::move(queryParameterTypes), gUCSettings)
{}

void TKqpQueryId::BuildKey(TStringBuf cluster, TStringBuf database, TStringBuf databaseId, TStringBuf userSid,
    TStringBuf encodedGUCSettings)
{
    const size_t headerSize = FieldCount * sizeof(ui32);
    // A rough estimate to avoid reallocations, not part of the format.
    const size_t sizeEstimate = headerSize + cluster.size() + database.size() + databaseId.size() + userSid.size()
        + encodedGUCSettings.size() + 64 + (QueryParameterTypes_ ? QueryParameterTypes_->size() * 24 : 0);
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
    Hash_ = CombineHashes(THash<TString>()(Key_), TextHash_);
}

const TKqpEncodedGUC& TKqpEncodedGUCSettings::Get(const TGUCSettings& gUCSettings) {
    if (!Cache_ || !(Cache_->first == gUCSettings)) {
        // Replaced as a whole only once encoding succeeded.
        auto encoded = std::make_pair(gUCSettings, TKqpEncodedGUC::Encode(gUCSettings));
        Cache_ = std::move(encoded);
    }
    return Cache_->second;
}

TStringBuf TKqpQueryId::Field(EField field) const {
    ui32 ends[FieldCount];
    // A moved-from key has no buffer: its fields read as empty.
    if (Key_.size() < sizeof(ends)) {
        return {};
    }
    memcpy(ends, Key_.data(), sizeof(ends));
    const ui32 begin = field == 0 ? sizeof(ends) : ends[field - 1];
    return TStringBuf(Key_.data() + begin, ends[field] - begin);
}

void TKqpQueryId::SetUserSid(TStringBuf userSid) {
    if (userSid != UserSid()) {
        BuildKey(Cluster(), Database(), DatabaseId(), userSid, Field(FieldGUCSettings));
    }
}

void TKqpQueryId::SetQueryParameterTypes(std::shared_ptr<const TParameterTypes> queryParameterTypes) {
    QueryParameterTypes_ = std::move(queryParameterTypes);
    BuildKey(Cluster(), Database(), DatabaseId(), UserSid(), Field(FieldGUCSettings));
}

TGUCSettings TKqpQueryId::DecodeGUCSettings() const {
    return TGUCSettings(TString(Field(FieldGUCSettings)));
}

bool TKqpQueryId::IsSql() const {
    return IsSqlQuery(Settings_.QueryType);
}

bool TKqpQueryId::operator==(const TKqpQueryId& other) const {
    // Different keys almost always differ in the hash, so a long text is not walked.
    return Hash_ == other.Hash_ && DisablePgCompatibility_ == other.DisablePgCompatibility_
        && Key_ == other.Key_ && Text_ == other.Text_;
}

TString TKqpQueryId::SerializeToString() const {
    TStringBuilder result = TStringBuilder() << "{"
        << "Cluster: " << Cluster() << ", "
        << "Database: " << Database() << ", "
        << "DatabaseId: " << DatabaseId() << ", "
        << "UserSid: " << UserSid() << ", "
        << "Text: " << EscapeC(Text()) << ", "
        << "Settings: " << Settings_.SerializeToString() << ", "
        << "DisablePgCompatibility: " << DisablePgCompatibility_ << ", ";
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
