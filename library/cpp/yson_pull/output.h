#pragma once

#include "buffer.h"

#include <util/generic/ptr.h>
#include <util/generic/strbuf.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <cstddef>
#include <cstdio>
#include <cstring>
#include <memory>

//! \brief Output stream adaptor interface.
//!
//! Represents a model of an optionally-buffered writer.
namespace NYsonPull {
    namespace NOutput {
        class IStream {
            output_buffer buffer_;

        public:
            virtual ~IStream() = default;

            output_buffer& buffer() noexcept {
                return buffer_;
            }
            const output_buffer& buffer() const noexcept {
                return buffer_;
            }

            void flush_buffer(TStringBuf extra = {}) {
                if (!extra.empty() || !buffer_.is_empty()) {
                    do_flush_buffer(extra);
                }
                while (!buffer_.is_empty()) {
                    do_flush_buffer({});
                }
            }

        protected:
            //! \brief Flush data to underlying stream.
            //!
            //! The implementation is to flush the buffer contents AND
            //! extra argument to underlying stream.
            //!
            //! This way, at zero buffer size this interface implements an unbuffered
            //! stream (with an added cost of a virtual call per each write).
            //!
            //! Write is assumed to always succeed unless it throws an exception.
            virtual void do_flush_buffer(TStringBuf extra) = 0;
        };

        //! \brief Write data to C FILE* object.
        THolder<IStream> FromStdioFile(FILE* file, size_t buffer_size = 0);

        //! \brief Write data to POSIX file descriptor
        THolder<IStream> FromPosixFd(int fd, size_t buffer_size = 65536);

        THolder<IStream> FromOutputStream(IOutputStream* output, size_t buffer_size = 65536);

        THolder<IStream> FromString(TString* output, size_t buffer_size = 1024);
    }
}
