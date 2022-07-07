#pragma once

#include <util/generic/string.h>

struct TEntity {
    size_t Len;
    wchar32 Codepoint1;
    wchar32 Codepoint2;
};

constexpr size_t MaxNamedEntityLength = 32; //CounterClockwiseContourIntegral;

//! Find if string prefix may be considered as entity name.
//! (';' is a part of entity name)
//! @param inp - a pointer after '&'.
//! @param len - inspected string leng (may be more than simple entity).
bool DecodeNamedEntity(const unsigned char* inp, size_t len, TEntity*);
