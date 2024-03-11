from libcpp cimport bool

from util.generic.maybe cimport TMaybe
from util.generic.string cimport TStringBuf, TString


cdef extern from "library/cpp/monlib/metrics/labels.h" namespace "NMonitoring" nogil:
    cdef cppclass ILabel:
        const TStringBuf Name() const
        const TStringBuf Value() const

    cdef cppclass ILabels:
        bool Add(TStringBuf name, TStringBuf value)
        bool Add(const TString& name, const TString& value)

        size_t Size() const

    cdef cppclass TLabel:
        TLabel() except +
        TLabel(TStringBuf name, TStringBuf value) except +
        const TString& Name() const
        const TString& Value() const

        TString ToString() const
        bool operator!=(const TLabel&) const
        bool operator==(const TLabel&) const

    cdef cppclass TLabels:
        cppclass const_iterator:
            const TLabel& operator*() const
            bool operator!=(const_iterator) const
            bool operator==(const_iterator) const

        TLabels() except +

        bool Add(const TLabel&) except +
        bool Add(TStringBuf name, TStringBuf value) except +
        bool Add(const TString& name, const TString& value) except +
        bool operator==(const TLabels&) const

        TMaybe[TLabel] Find(TStringBuf name) const
        TMaybe[TLabel] Extract(TStringBuf name) except +

        size_t Size() const

        const_iterator begin() const
        const_iterator end() const
