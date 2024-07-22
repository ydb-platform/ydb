#pragma once

#include <ydb/library/yql/minikql/computation/mkql_spiller_factory.h>
#include <ydb/library/yql/minikql/computation/mock_spiller_ut.h>

namespace NKikimr::NMiniKQL {

using namespace NActors;

class TMockSpillerFactory : public ISpillerFactory
{
public:
    ISpiller::TPtr CreateSpiller() override {
        return CreateMockSpiller();
    }
};

} // namespace NKikimr::NMiniKQL