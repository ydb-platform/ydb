#pragma once

#include "buffered.h"

#include <library/cpp/yson_pull/detail/macros.h>

#include <library/cpp/yson_pull/exceptions.h>
#include <library/cpp/yson_pull/input.h>

#include <cstdio>
#include <memory>

namespace NYsonPull {
    namespace NDetail {
        namespace NInput {
            class TStdioFile: public TBuffered {
                FILE* file_;

            public:
                TStdioFile(FILE* file, size_t buffer_size)
                    : TBuffered(buffer_size)
                    , file_{file} {
                }

            protected:
                result do_fill_buffer() override {
                    auto nread = ::fread(buffer_data(), 1, buffer_size(), file_);
                    if (Y_UNLIKELY(nread == 0)) {
                        if (ferror(file_)) {
                            throw NException::TSystemError();
                        }
                        if (feof(file_)) {
                            return result::at_end;
                        }
                    }
                    buffer().reset(buffer_data(), buffer_data() + nread);
                    return result::have_more_data;
                }
            };
        }
    }     // namespace NDetail
}
