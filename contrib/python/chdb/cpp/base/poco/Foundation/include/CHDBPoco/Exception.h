//
// Exception.h
//
// Library: Foundation
// Package: Core
// Module:  Exception
//
// Definition of various Poco exception classes.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Foundation_Exception_INCLUDED
#define CHDB_Foundation_Exception_INCLUDED


#include <stdexcept>
#include "CHDBPoco/Foundation.h"


namespace CHDBPoco
{


class Foundation_API Exception : public std::exception
/// This is the base class for all exceptions defined
/// in the Poco class library.
{
public:
    Exception(const std::string & msg, int code = 0);
    /// Creates an exception.

    Exception(std::string && msg, int code = 0);
    /// Creates an exception.

    Exception(const std::string & msg, const std::string & arg, int code = 0);
    /// Creates an exception.

    Exception(const std::string & msg, const Exception & nested, int code = 0);
    /// Creates an exception and stores a clone
    /// of the nested exception.

    Exception(const Exception & exc);
    /// Copy constructor.

    ~Exception() throw();
    /// Destroys the exception and deletes the nested exception.

    Exception & operator=(const Exception & exc);
    /// Assignment operator.

    virtual const char * name() const throw();
    /// Returns a static string describing the exception.

    virtual const char * className() const throw();
    /// Returns the name of the exception class.

    virtual const char * what() const throw();
    /// Returns a static string describing the exception.
    ///
    /// Same as name(), but for compatibility with std::exception.

    const Exception * nested() const;
    /// Returns a pointer to the nested exception, or
    /// null if no nested exception exists.

    const std::string & message() const;
    /// Returns the message text.

    int code() const;
    /// Returns the exception code if defined.

    virtual std::string displayText() const;
    /// Returns a string consisting of the
    /// message name and the message text.

    virtual Exception * clone() const;
    /// Creates an exact copy of the exception.
    ///
    /// The copy can later be thrown again by
    /// invoking rethrow() on it.

    virtual void rethrow() const;
    /// (Re)Throws the exception.
    ///
    /// This is useful for temporarily storing a
    /// copy of an exception (see clone()), then
    /// throwing it again.

protected:
    Exception(int code = 0);
    /// Standard constructor.

    void message(const std::string & msg);
    /// Sets the message for the exception.

    void extendedMessage(const std::string & arg);
    /// Sets the extended message for the exception.

private:
    std::string _msg;
    Exception * _pNested;
    int _code;
};


//
// inlines
//
inline const Exception * Exception::nested() const
{
    return _pNested;
}


inline const std::string & Exception::message() const
{
    return _msg;
}


inline void Exception::message(const std::string & msg)
{
    _msg = msg;
}


inline int Exception::code() const
{
    return _code;
}


//
// Macros for quickly declaring and implementing exception classes.
// Unfortunately, we cannot use a template here because character
// pointers (which we need for specifying the exception name)
// are not allowed as template arguments.
//
#define CHDB_POCO_DECLARE_EXCEPTION_CODE(API, CLS, BASE, CODE) \
    class API CLS : public BASE \
    { \
    public: \
        CLS(int code = CODE); \
        CLS(const std::string & msg, int code = CODE); \
        CLS(const std::string & msg, const std::string & arg, int code = CODE); \
        CLS(const std::string & msg, const CHDBPoco::Exception & exc, int code = CODE); \
        CLS(const CLS & exc); \
        ~CLS() throw(); \
        CLS & operator=(const CLS & exc); \
        const char * name() const throw(); \
        const char * className() const throw(); \
        CHDBPoco::Exception * clone() const; \
        void rethrow() const; \
    };

#define CHDB_POCO_DECLARE_EXCEPTION(API, CLS, BASE) CHDB_POCO_DECLARE_EXCEPTION_CODE(API, CLS, BASE, 0)

#define CHDB_POCO_IMPLEMENT_EXCEPTION(CLS, BASE, NAME) \
    CLS::CLS(int code) : BASE(code) \
    { \
    } \
    CLS::CLS(const std::string & msg, int code) : BASE(msg, code) \
    { \
    } \
    CLS::CLS(const std::string & msg, const std::string & arg, int code) : BASE(msg, arg, code) \
    { \
    } \
    CLS::CLS(const std::string & msg, const CHDBPoco::Exception & exc, int code) : BASE(msg, exc, code) \
    { \
    } \
    CLS::CLS(const CLS & exc) : BASE(exc) \
    { \
    } \
    CLS::~CLS() throw() \
    { \
    } \
    CLS & CLS::operator=(const CLS & exc) \
    { \
        BASE::operator=(exc); \
        return *this; \
    } \
    const char * CLS::name() const throw() \
    { \
        return NAME; \
    } \
    const char * CLS::className() const throw() \
    { \
        return typeid(*this).name(); \
    } \
    CHDBPoco::Exception * CLS::clone() const \
    { \
        return new CLS(*this); \
    } \
    void CLS::rethrow() const \
    { \
        throw *this; \
    }


//
// Standard exception classes
//
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, LogicException, Exception)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, AssertionViolationException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, NullPointerException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, NullValueException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, BugcheckException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, InvalidArgumentException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, NotImplementedException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, RangeException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, IllegalStateException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, InvalidAccessException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, SignalException, LogicException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, UnhandledException, LogicException)

CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, RuntimeException, Exception)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, NotFoundException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, ExistsException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, TimeoutException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, SystemException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, RegularExpressionException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, LibraryLoadException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, LibraryAlreadyLoadedException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, NoThreadAvailableException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, PropertyNotSupportedException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, PoolOverflowException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, NoPermissionException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, OutOfMemoryException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, DataException, RuntimeException)

CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, DataFormatException, DataException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, SyntaxException, DataException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, CircularReferenceException, DataException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, PathSyntaxException, SyntaxException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, IOException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, ProtocolException, IOException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, FileException, IOException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, FileExistsException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, FileNotFoundException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, PathNotFoundException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, FileReadOnlyException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, FileAccessDeniedException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, CreateFileException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, OpenFileException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, WriteFileException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, ReadFileException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, DirectoryNotEmptyException, FileException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, UnknownURISchemeException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, TooManyURIRedirectsException, RuntimeException)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, URISyntaxException, SyntaxException)

CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, ApplicationException, Exception)
CHDB_POCO_DECLARE_EXCEPTION(Foundation_API, BadCastException, RuntimeException)


} // namespace CHDBPoco


#endif // CHDB_Foundation_Exception_INCLUDED
