//
// ValidArgs.h
//
// Library: Foundation
// Package: Cache
// Module:  ValidArgs
//
// Definition of the ValidArgs class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DB_Foundation_ValidArgs_INCLUDED
#define DB_Foundation_ValidArgs_INCLUDED


#include "DBPoco/Foundation.h"


namespace DBPoco
{


template <class TKey>
class ValidArgs
{
public:
    ValidArgs(const TKey & key) : _key(key), _isValid(true) { }

    ValidArgs(const ValidArgs & args) : _key(args._key), _isValid(args._isValid) { }

    ~ValidArgs() { }

    const TKey & key() const { return _key; }

    bool isValid() const { return _isValid; }

    void invalidate() { _isValid = false; }

protected:
    const TKey & _key;
    bool _isValid;

private:
    ValidArgs & operator=(const ValidArgs & args);
};


} // namespace DBPoco


#endif // DB_Foundation_ValidArgs_INCLUDED
