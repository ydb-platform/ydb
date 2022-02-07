#pragma once

#include "buffered.h"

#include <library/cpp/yson_pull/detail/macros.h>

#include <library/cpp/yson_pull/exceptions.h>

#include <cstdio>

namespace NYsonPull {
    namespace NDetail {
        namespace NOutput {
            class TStdioFile: public TBuffered<TStdioFile> {
                FILE* file_;

            public:
                TStdioFile(FILE* file, size_t buffer_size)
                    : TBuffered<TStdioFile>(buffer_size)
                    , file_(file)
                {
                }

                void write(TStringBuf data) {
                    auto nwritten = ::fwrite(data.data(), 1, data.size(), file_);
                    if (Y_UNLIKELY(static_cast<size_t>(nwritten) != data.size())) {
                        throw NException::TSystemError();
                    }
                }
            };
        }
    }     // namespace NDetail
}
