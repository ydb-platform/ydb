#pragma once

#include "buffer.h"

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <memory>

class IInputStream;
class IZeroCopyInput;

namespace NYsonPull {
    namespace NInput {
        //! \brief Input stream adaptor interface.
        //!
        //! Represents a model of a chunked input data stream.
        class IStream {
            input_buffer buffer_;
            bool at_end_ = false;

        public:
            virtual ~IStream() = default;

            bool at_end() const {
                return at_end_;
            }

            input_buffer& buffer() noexcept {
                return buffer_;
            }
            const input_buffer& buffer() const noexcept {
                return buffer_;
            }

            void fill_buffer() {
                while (buffer_.is_empty() && !at_end()) {
                    at_end_ = do_fill_buffer() == result::at_end;
                }
            }

        protected:
            enum class result {
                have_more_data, //! May continue reading
                at_end,         //! Reached end of stream
            };

            //! \brief Read next chunk of data.
            //!
            //! The implementation is to discard the buffer contents
            //! and reset the buffer to a next chunk of data.
            //! End-of-stream condition is to be reported via return value.
            //!
            //! Read is assumed to always succeed unless it throws an exception.
            virtual result do_fill_buffer() = 0;
        };

        //! \brief Read data from a contiguous memory block (i.e. a string)
        //!
        //! Does not take ownership on memory.
        THolder<IStream> FromMemory(TStringBuf data);

        //! \brief Read data from C FILE* object.
        //!
        //! Does not take ownership on file object.
        //! Data is buffered internally regardless of file buffering.
        THolder<IStream> FromStdioFile(FILE* file, size_t buffer_size = 65536);

        //! \brief Read data from POSIX file descriptor.
        //!
        //! Does not take ownership on streambuf.
        THolder<IStream> FromPosixFd(int fd, size_t buffer_size = 65536);

        THolder<IStream> FromZeroCopyInput(IZeroCopyInput* input);

        THolder<IStream> FromInputStream(IInputStream* input, size_t buffer_size = 65536);
    }
}
