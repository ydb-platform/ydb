#ifndef JSON5EncoderCpp_StackHeapString
#define JSON5EncoderCpp_StackHeapString

#include <cstdint>
#include <cstring>
#include <Python.h>


namespace JSON5EncoderCpp {
inline namespace {

static constexpr Py_ssize_t StackHeapStringStackSize = 64;
static constexpr Py_ssize_t StackHeapStringHeapSize = 256;
static constexpr Py_ssize_t StackHeapStringHeapFactor = 4;


template <class T>
class StackHeapString {
    StackHeapString(const StackHeapString&) = delete;
    StackHeapString(StackHeapString&&) = delete;
    StackHeapString &operator =(const StackHeapString&) = delete;
    StackHeapString &operator =(StackHeapString&&) = delete;

public:
    StackHeapString() = default;

    ~StackHeapString() {
        if (m_heap != nullptr) {
            PyMem_RawFree(m_heap);
        }
    }

    const T *data() const& {
        if (JSON5EncoderCpp_expect(m_heap == nullptr, true)) {
            return m_stack;
        } else {
            return m_heap;
        }
    }

    Py_ssize_t size() const& {
        return m_size;
    }

    bool push_back(T c) {
        if (JSON5EncoderCpp_expect(m_left == 0, false)) {
            if (m_heap == nullptr) {
                void *new_ptr = PyMem_RawMalloc(sizeof(T) * StackHeapStringHeapSize);
                if (new_ptr == nullptr) {
                    PyErr_NoMemory();
                    return false;
                }

                m_heap = reinterpret_cast<T*>(new_ptr);
                m_left = StackHeapStringHeapSize - StackHeapStringStackSize;
                std::memcpy(m_heap, m_stack, sizeof(T) * StackHeapStringStackSize);
            } else {
                void *new_ptr = PyMem_RawRealloc(m_heap, sizeof(T) * (m_size * StackHeapStringHeapFactor));
                if (new_ptr == nullptr) {
                    PyErr_NoMemory();
                    return false;
                }

                m_heap = reinterpret_cast<T*>(new_ptr);
                m_left = m_size * (StackHeapStringHeapFactor - 1);
            }
        }

        if (JSON5EncoderCpp_expect(m_heap == nullptr, true)) {
            m_stack[m_size] = c;
        } else {
            m_heap[m_size] = c;
        }

        ++m_size;
        --m_left;
        return true;
    }

private:
    Py_ssize_t m_size = 0;
    Py_ssize_t m_left = StackHeapStringStackSize;
    T *m_heap = nullptr;
    T m_stack[StackHeapStringStackSize];
};

}
}

#endif  // ifndef JSON5EncoderCpp_StackHeapString
