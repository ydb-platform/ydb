#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <cstddef>

namespace NYsonPull {
    //! \brief A non-owning buffer model.
    //!
    //! Represents a \p pos pointer moving between \p begin and \p end.
    template <typename T>
    class buffer {
        T* begin_ = nullptr;
        T* pos_ = nullptr;
        T* end_ = nullptr;

    public:
        T* begin() const noexcept {
            return begin_;
        }
        T* pos() const noexcept {
            return pos_;
        }
        T* end() const noexcept {
            return end_;
        }

        //! \brief Amount of data after current position.
        size_t available() const noexcept {
            return end_ - pos_;
        }

        //! \brief Amount of data before current position.
        size_t used() const noexcept {
            return pos_ - begin_;
        }

        //! \brief Move current position \p nbytes forward.
        void advance(size_t nbytes) noexcept {
            Y_ASSERT(pos_ + nbytes <= end_);
            pos_ += nbytes;
        }

        //! \brief Reset buffer pointers.
        void reset(T* new_begin, T* new_end, T* new_pos) {
            begin_ = new_begin;
            pos_ = new_pos;
            end_ = new_end;
        }

        //! \brief Reset buffer to beginning
        void reset(T* new_begin, T* new_end) {
            reset(new_begin, new_end, new_begin);
        }
    };

    class output_buffer: public buffer<ui8> {
    public:
        //! \brief An output buffer is empty when there is no data written to it.
        bool is_empty() const noexcept {
            return pos() == begin();
        }

        //! \brief An output buffer is full when there is no space to write more data to it.
        bool is_full() const noexcept {
            return pos() == end();
        }
    };

    class input_buffer: public buffer<const ui8> {
    public:
        //! An input stream is empty when there is no data to read in it.
        bool is_empty() const noexcept {
            return pos() == end();
        }
    };

}
