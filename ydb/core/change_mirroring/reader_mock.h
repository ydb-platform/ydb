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

    void NeedPollResult(AIReader::Tag, const AIReader::TEvReader::TEvNeedPollResult& result) override {
        if (OnNeedPollResult) {
            OnNeedPollResult(*this, result);
        }
    }

    void PollResult(AIReader::Tag) override {
        if (OnPollResult) {
            OnPollResult(*this);
        }
    }

    void RemainingResult(AIReader::Tag, const AIReader::TEvReader::TEvRemainingResult& result) override {
        if (OnRemainingResult) {
            OnRemainingResult(*this, result);
        }
    }

    void ReadNextResult(AIReader::Tag, const AIReader::TEvReader::TEvReadNextResult& result) override {
        if (OnReadNextResult) {
            OnReadNextResult(*this, result);
        }
    }

    std::function<void(TReaderClientMock&)> OnBootstrap;
    std::function<void(TReaderClientMock&, const AIReader::TEvReader::TEvNeedPollResult&)> OnNeedPollResult;
    std::function<void(TReaderClientMock&)> OnPollResult;
    std::function<void(TReaderClientMock&, const AIReader::TEvReader::TEvRemainingResult&)> OnRemainingResult;
    std::function<void(TReaderClientMock&, const AIReader::TEvReader::TEvReadNextResult&)> OnReadNextResult;
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

    void NeedPoll(AIReader::Tag) override {
        if (OnNeedPoll) {
            OnNeedPoll(*this);
        }
    }

    void Poll(AIReader::Tag) override {
        if (OnPoll) {
            OnPoll(*this);
        }
    }

    void Remaining(AIReader::Tag) override {
        if (OnRemaining) {
            OnRemaining(*this);
        }
    }

    void ReadNext(AIReader::Tag) override {
        if (OnReadNext) {
            OnReadNext(*this);
        }
    }

    std::function<void(TReaderServerMock&)> OnBootstrap;
    std::function<void(TReaderServerMock&)> OnNeedPoll;
    std::function<void(TReaderServerMock&)> OnPoll;
    std::function<void(TReaderServerMock&)> OnRemaining;
    std::function<void(TReaderServerMock&)> OnReadNext;
};

} // namespace NKikimr::NChangeMirroring
