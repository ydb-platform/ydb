#pragma once

#include "buffered.h"

#include <library/cpp/yson_pull/detail/macros.h>
#include <library/cpp/yson_pull/exceptions.h>

#include <util/stream/output.h>
#include <util/stream/file.h>
#include <util/system/file.h>

namespace NYsonPull {
    namespace NDetail {
        namespace NOutput {
            class TStream: public TBuffered<TStream> {
                IOutputStream* Output;

            public:
                TStream(IOutputStream* output, size_t buffer_size)
                    : TBuffered<TStream>(buffer_size)
                    , Output(output)
                {
                }

                void write(TStringBuf data) {
                    Output->Write(data);
                }
            };

            template <typename TOutput>
            class TOwned: public TBuffered<TOwned<TOutput>> {
                TOutput Output;

            public:
                template <typename... Args>
                TOwned(size_t buffer_size, Args&&... args)
                    : TBuffered<TOwned>(buffer_size)
                    , Output(std::forward<Args>(args)...)
                {
                }

                void write(TStringBuf data) {
                    Output.Write(data);
                }
            };

            class TFHandle: public TOwned<TUnbufferedFileOutput> {
            public:
                TFHandle(int fd, size_t buffer_size)
                    : TOwned<TUnbufferedFileOutput>(buffer_size, Duplicate(fd))
                {
                }
            };
        }
    }     // namespace NDetail
}
