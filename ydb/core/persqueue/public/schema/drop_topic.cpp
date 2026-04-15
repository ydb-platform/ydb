#include "schema_int.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/public/constants.h>
#include <ydb/core/persqueue/public/utils.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

namespace NKikimr::NPQ::NSchema {

namespace {

struct TDropTopicStrategy: public IDropTopicStrategy {
    TDropTopicStrategy(Ydb::Topic::DropTopicRequest&& request)
        : Request(std::move(request))
    {
    }

    const TString& GetTopicName() const override {
        return Request.path();
    }

    Ydb::Topic::DropTopicRequest Request;
};

} // namespace

NActors::IActor* CreateDropTopicActor(const NActors::TActorId& parentId, TDropTopicSettings&& settings) {
    return CreateTopicAlterer(NKikimrServices::EServiceKikimr::PQ_ALTER_TOPIC, TTopicAltererSettings{
        .ParentId = parentId,
        .Database = std::move(settings.Database),
        .PeerName = std::move(settings.PeerName),
        .UserToken = std::move(settings.UserToken),
        .Strategy = std::make_unique<TDropTopicStrategy>(std::move(settings.Request)),
        .IfExists = settings.IfExists,
        .Cookie = settings.Cookie,
    });
}

} // namespace NKikimr::NPQ::NSchema
