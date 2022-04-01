#include "dq_transform_actor_factory.h"

#include <ydb/library/yql/utils/yql_panic.h>
#include <google/protobuf/text_format.h>

namespace NYql::NDq {

TDqTransformActorFactory::TDqTransformActorFactory()
{}

std::pair<IDqTransformActor*, NActors::IActor*> TDqTransformActorFactory::CreateDqTransformActor(const NDqProto::TDqTransform& transform, TArguments&& args) {
    auto creator = CreatorsByType.find(transform.GetType());
    if (creator == CreatorsByType.end()) {
        YQL_ENSURE(false, "Unregistered type of transform actor: \"" << transform.GetType() << "\"");
    }

    std::pair<IDqTransformActor*, NActors::IActor*> actor = (creator->second)(transform, std::move(args));
    Y_VERIFY(actor.first);
    Y_VERIFY(actor.second);
    return actor;
}

void TDqTransformActorFactory::Register(TString type, TTransformCreator creator) {
    auto [_, registered] = CreatorsByType.emplace(type, std::move(creator));
    Y_VERIFY(registered);
}

}
