#pragma once

#include "backend.h"
#include <library/cpp/logger/backend_creator.h>

namespace NUnifiedAgent {

    class TLogBackendCreator: public TLogBackendCreatorBase {
    public:
        TLogBackendCreator();
        bool Init(const IInitContext& ctx) override;
        static TFactory::TRegistrator<TLogBackendCreator> Registrar;

    protected:
        void DoToJson(NJson::TJsonValue& value) const override;

    private:
        THolder<TLogBackend> DoCreateLogBackend() const override;

    private:
        THolder<TClientParameters> ClientParams;
        THolder<ILogBackendCreator> OwnLogger;
    };

}
