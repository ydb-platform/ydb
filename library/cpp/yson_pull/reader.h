#pragma once

#include "event.h"
#include "input.h"
#include "stream_type.h"

#include <util/system/yassert.h>

#include <memory>

namespace NYsonPull {
    namespace NDetail {
        class reader_impl;
    }

    //! \brief YSON reader facade class.
    //!
    //! Owns an input stream.
    class TReader {
        THolder<NInput::IStream> Stream_;
        THolder<NDetail::reader_impl> Impl_;

    public:
        TReader(THolder<NInput::IStream> stream, EStreamType mode);
        TReader(TReader&&) noexcept;
        ~TReader();

        //! \brief Advance stream to next event and return it.
        //!
        //! Any event data is invalidated by a call to NextEvent();
        const TEvent& NextEvent();

        //! \brief Get last returned event.
        const TEvent& LastEvent() const noexcept;
    };

}
