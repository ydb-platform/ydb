#pragma once

#include <library/cpp/yson_pull/detail/macros.h>

#include <library/cpp/yson_pull/input.h>

#include <util/stream/buffered.h>
#include <util/stream/file.h>
#include <util/stream/zerocopy.h>
#include <util/system/file.h>

namespace NYsonPull {
    namespace NDetail {
        namespace NInput {
            class TStreamBase: public NYsonPull::NInput::IStream {
            protected:
                result DoFillBufferFrom(IZeroCopyInput& input) {
                    void* ptr = nullptr;
                    size_t size = input.Next(&ptr);
                    if (Y_UNLIKELY(size == 0)) {
                        return result::at_end;
                    }
                    buffer().reset(static_cast<ui8*>(ptr), static_cast<ui8*>(ptr) + size);
                    return result::have_more_data;
                }
            };

            class TZeroCopy: public TStreamBase {
                IZeroCopyInput* Input;

            public:
                explicit TZeroCopy(IZeroCopyInput* input)
                    : Input(input)
                {
                }

            protected:
                result do_fill_buffer() override {
                    return DoFillBufferFrom(*Input);
                }
            };

            template <typename TBuffered>
            class TOwned: public TStreamBase {
                TBuffered Input;

            public:
                template <typename... Args>
                explicit TOwned(Args&&... args)
                    : Input(std::forward<Args>(args)...)
                {
                }

            protected:
                result do_fill_buffer() override {
                    return DoFillBufferFrom(Input);
                }
            };

            class TFHandle: public TOwned<TFileInput> {
            public:
                TFHandle(int fd, size_t buffer_size)
                    : TOwned<TFileInput>(Duplicate(fd), buffer_size)
                {
                }
            };
        }
    }     // namespace NDetail
}
