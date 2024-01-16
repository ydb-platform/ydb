#pragma once

#include "reader.h"

#include <functional>

namespace NKikimr::NChangeMirroring {

class TReaderClientMock
    : public NActors::TActorBootstrapped<TReaderClientMock>
    , public AIReader::TDefaultClientBase<TReaderClientMock>
{
public:
    using TBase::TBase;

    /* both can be reduced by patching actorlib */
    friend TBase;
    DEFINE_DERIVED_STATEFN();

    void Bootstrap() {
        if (OnBootstrap) {
            OnBootstrap(*this);
        }
    }

    void PollResult(AIReader::Tag, const AIReader::TEvReader::TEvPollResult& result) override {
        if (OnPollResult) {
            OnPollResult(*this, result);
        }
    }

    std::function<void(TReaderClientMock&)> OnBootstrap;
    std::function<void(TReaderClientMock&, const AIReader::TEvReader::TEvPollResult&)> OnPollResult;
};

class TReaderServerMock
    : public NActors::TActorBootstrapped<TReaderServerMock>
    , public AIReader::TDefaultServerBase<TReaderServerMock>
{
public:
    using TBase::TBase;

    /* both can be reduced by patching actorlib */
    friend TBase;
    DEFINE_DERIVED_STATEFN();

    void Bootstrap() {
        if (OnBootstrap) {
            OnBootstrap(*this);
        }
    }

    void Poll(AIReader::Tag, const AIReader::TEvReader::TEvPoll::TPtr& result) override {
        if (OnPoll) {
            OnPoll(*this, result);
        }
    }

    std::function<void(TReaderServerMock&)> OnBootstrap;
    std::function<void(TReaderServerMock&, const AIReader::TEvReader::TEvPoll::TPtr&)> OnPoll;
};

} // namespace NKikimr::NChangeMirroring
