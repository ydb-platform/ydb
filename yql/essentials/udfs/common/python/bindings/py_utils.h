#pragma once

#include "py_ptr.h"

#include <util/generic/strbuf.h>

#ifdef _win_
#define INIT_MEMBER(member, value) value //member
#else
#define INIT_MEMBER(member, value) .member = (value)
#endif

namespace NPython {

TPyObjectPtr PyRepr(TStringBuf asciiStr, bool intern = false);

template <size_t size>
TPyObjectPtr PyRepr(const char(&str)[size]) {
    return PyRepr(TStringBuf(str, size - 1), true);
}

TString PyObjectRepr(PyObject* value);

bool HasEncodingCookie(const TString& source);

void PyCleanup();

} // namspace NPython
