#pragma once

#include <library/cpp/unified_agent_client/client.h>

#include <library/cpp/logger/backend.h>
#include <library/cpp/logger/log.h>

namespace NUnifiedAgent {
    class IRecordConverter {
    public:
        virtual ~IRecordConverter() = default;

        virtual TClientMessage Convert(const TLogRecord&) const = 0;
    };

    THolder<IRecordConverter> MakeDefaultRecordConverter(bool stripTrailingNewLine = true);

    THolder<TLogBackend> AsLogBackend(const TClientSessionPtr& session, bool stripTrailingNewLine = true);

    THolder<TLogBackend> MakeLogBackend(const TClientParameters& parameters,
        const TSessionParameters& sessionParameters = {},
        THolder<IRecordConverter> recordConverter = {});

    THolder<::TLog> MakeLog(const TClientParameters& parameters,
        const TSessionParameters& sessionParameters = {},
        THolder<IRecordConverter> recordConverter = {});
}
