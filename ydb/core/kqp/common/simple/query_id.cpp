#include "query_id.h"
#include "helpers.h"

#include <google/protobuf/util/message_differencer.h>

#include <util/generic/yexception.h>

#include <memory>

namespace NKikimr::NKqp {

TKqpQueryId::TKqpQueryId(const TString& cluster, const TString& database, const TString& text,
    const TKqpQuerySettings& settings, std::shared_ptr<std::map<TString, Ydb::Type>> queryParameterTypes,
    const TGUCSettings& gUCSettings)
    : Cluster(cluster)
    , Database(database)
    , Text(text)
    , Settings(settings)
    , QueryParameterTypes(queryParameterTypes)
    , GUCSettings(gUCSettings)
{
    switch (Settings.QueryType) {
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
}

bool TKqpQueryId::IsSql() const {
    return IsSqlQuery(Settings.QueryType);
}

bool TKqpQueryId::operator==(const TKqpQueryId& other) const {
    if (!(Cluster == other.Cluster &&
        Database == other.Database &&
        UserSid == other.UserSid &&
        Text == other.Text &&
        Settings == other.Settings &&
        !QueryParameterTypes == !other.QueryParameterTypes &&
        GUCSettings == other.GUCSettings)) {
        return false;
    }

    if (!QueryParameterTypes) {
        return true;
    }

    if (QueryParameterTypes->size() != other.QueryParameterTypes->size()) {
        return false;
    }

    for (auto it = QueryParameterTypes->begin(), otherIt = other.QueryParameterTypes->begin(); it != QueryParameterTypes->end(); ++it, ++otherIt) {
        if (it->first != otherIt->first) {
            return false;
        }

        const auto& type = it->second;
        const auto& otherType = otherIt->second;

        // we can't use type.SerializeAsString() == otherType.SerializeAsString() here since serialization of protobufs is unstable
        if (!google::protobuf::util::MessageDifferencer::Equals(type, otherType)) {
            return false;
        }
    }

    return true;
}

} // namespace NKikimr::NKqp
