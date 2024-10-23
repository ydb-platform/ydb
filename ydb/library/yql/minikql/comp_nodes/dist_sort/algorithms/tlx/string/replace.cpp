/*******************************************************************************
 * tlx/string/replace.cpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2007-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#include <tlx/string/replace.hpp>

#include <algorithm>
#include <cstring>

namespace tlx {

/******************************************************************************/
// replace_first() in-place

std::string& replace_first(
    std::string* str, const std::string& needle, const std::string& instead) {

    std::string::size_type firstpos = str->find(needle);

    if (firstpos != std::string::npos)
        str->replace(firstpos, needle.size(), instead);

    return *str;
}

std::string& replace_first(
    std::string* str, const std::string& needle, const char* instead) {

    std::string::size_type firstpos = str->find(needle);

    if (firstpos != std::string::npos)
        str->replace(firstpos, needle.size(), instead);

    return *str;
}

std::string& replace_first(
    std::string* str, const char* needle, const std::string& instead) {

    std::string::size_type firstpos = str->find(needle);

    if (firstpos != std::string::npos)
        str->replace(firstpos, strlen(needle), instead);

    return *str;
}

std::string& replace_first(
    std::string* str, const char* needle, const char* instead) {

    std::string::size_type firstpos = str->find(needle);

    if (firstpos != std::string::npos)
        str->replace(firstpos, strlen(needle), instead);

    return *str;
}

std::string& replace_first(std::string* str, char needle, char instead) {

    std::string::size_type firstpos = str->find(needle);

    if (firstpos != std::string::npos)
        (*str)[firstpos] = instead;

    return *str;
}

/******************************************************************************/
// replace_first() copy

std::string replace_first(
    const std::string& str,
    const std::string& needle, const std::string& instead) {

    std::string newstr = str;
    std::string::size_type firstpos = newstr.find(needle);

    if (firstpos != std::string::npos)
        newstr.replace(firstpos, needle.size(), instead);

    return newstr;
}

std::string replace_first(
    const std::string& str, const std::string& needle, const char* instead) {

    std::string newstr = str;
    std::string::size_type firstpos = newstr.find(needle);

    if (firstpos != std::string::npos)
        newstr.replace(firstpos, needle.size(), instead);

    return newstr;
}

std::string replace_first(
    const std::string& str, const char* needle, const std::string& instead) {

    std::string newstr = str;
    std::string::size_type firstpos = newstr.find(needle);

    if (firstpos != std::string::npos)
        newstr.replace(firstpos, strlen(needle), instead);

    return newstr;
}

std::string replace_first(
    const std::string& str, const char* needle, const char* instead) {

    std::string newstr = str;
    std::string::size_type firstpos = newstr.find(needle);

    if (firstpos != std::string::npos)
        newstr.replace(firstpos, strlen(needle), instead);

    return newstr;
}

std::string replace_first(const std::string& str, char needle, char instead) {

    std::string newstr = str;
    std::string::size_type firstpos = newstr.find(needle);

    if (firstpos != std::string::npos)
        newstr[firstpos] = instead;

    return newstr;
}

/******************************************************************************/
// replace_all() in-place

std::string& replace_all(
    std::string* str, const std::string& needle, const std::string& instead) {

    std::string::size_type lastpos = 0, thispos;

    while ((thispos = str->find(needle, lastpos)) != std::string::npos)
    {
        str->replace(thispos, needle.size(), instead);
        lastpos = thispos + instead.size();
    }
    return *str;
}

std::string& replace_all(
    std::string* str, const std::string& needle, const char* instead) {

    std::string::size_type lastpos = 0, thispos;
    size_t instead_size = strlen(instead);

    while ((thispos = str->find(needle, lastpos)) != std::string::npos)
    {
        str->replace(thispos, needle.size(), instead);
        lastpos = thispos + instead_size;
    }
    return *str;
}

std::string& replace_all(
    std::string* str, const char* needle, const std::string& instead) {

    std::string::size_type lastpos = 0, thispos;
    size_t needle_size = strlen(needle);

    while ((thispos = str->find(needle, lastpos)) != std::string::npos)
    {
        str->replace(thispos, needle_size, instead);
        lastpos = thispos + instead.size();
    }
    return *str;
}

std::string& replace_all(
    std::string* str, const char* needle, const char* instead) {

    std::string::size_type lastpos = 0, thispos;
    size_t needle_size = strlen(needle);
    size_t instead_size = strlen(instead);

    while ((thispos = str->find(needle, lastpos)) != std::string::npos)
    {
        str->replace(thispos, needle_size, instead);
        lastpos = thispos + instead_size;
    }
    return *str;
}

std::string& replace_all(std::string* str, char needle, char instead) {

    std::string::size_type lastpos = 0, thispos;

    while ((thispos = str->find(needle, lastpos)) != std::string::npos)
    {
        (*str)[thispos] = instead;
        lastpos = thispos + 1;
    }
    return *str;
}

/******************************************************************************/
// replace_all() copy

std::string replace_all(
    const std::string& str,
    const std::string& needle, const std::string& instead) {

    std::string newstr = str;
    std::string::size_type lastpos = 0, thispos;

    while ((thispos = newstr.find(needle, lastpos)) != std::string::npos)
    {
        newstr.replace(thispos, needle.size(), instead);
        lastpos = thispos + instead.size();
    }
    return newstr;
}

std::string replace_all(
    const std::string& str, const std::string& needle, const char* instead) {

    std::string newstr = str;
    std::string::size_type lastpos = 0, thispos;
    size_t instead_size = strlen(instead);

    while ((thispos = newstr.find(needle, lastpos)) != std::string::npos)
    {
        newstr.replace(thispos, needle.size(), instead);
        lastpos = thispos + instead_size;
    }
    return newstr;
}

std::string replace_all(
    const std::string& str, const char* needle, const std::string& instead) {

    std::string newstr = str;
    std::string::size_type lastpos = 0, thispos;
    size_t needle_size = strlen(needle);

    while ((thispos = newstr.find(needle, lastpos)) != std::string::npos)
    {
        newstr.replace(thispos, needle_size, instead);
        lastpos = thispos + instead.size();
    }
    return newstr;
}

std::string replace_all(
    const std::string& str, const char* needle, const char* instead) {

    std::string newstr = str;
    std::string::size_type lastpos = 0, thispos;
    size_t needle_size = strlen(needle);
    size_t instead_size = strlen(instead);

    while ((thispos = newstr.find(needle, lastpos)) != std::string::npos)
    {
        newstr.replace(thispos, needle_size, instead);
        lastpos = thispos + instead_size;
    }
    return newstr;
}

std::string replace_all(const std::string& str, char needle, char instead) {

    std::string newstr = str;
    std::string::size_type lastpos = 0, thispos;

    while ((thispos = newstr.find(needle, lastpos)) != std::string::npos)
    {
        newstr[thispos] = instead;
        lastpos = thispos + 1;
    }
    return newstr;
}

} // namespace tlx

/******************************************************************************/
