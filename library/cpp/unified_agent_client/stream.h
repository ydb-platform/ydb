#pragma once

#include <library/cpp/unified_agent_client/client.h>

namespace NUnifiedAgent {
    class IStreamRecordConverter {
    public:
        virtual ~IStreamRecordConverter() = default;

        virtual TClientMessage Convert(const void* buf, size_t len) const = 0;
    };

    THolder<IStreamRecordConverter> MakeDefaultStreamRecordConverter(bool stripTrailingNewLine = true);

    THolder<IOutputStream> MakeOutputStream(const TClientParameters& parameters,
        const TSessionParameters& sessionParameters = {},
        THolder<IStreamRecordConverter> recordConverter = {});
}
