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


#ifndef DB_Foundation_Exception_INCLUDED
#define DB_Foundation_Exception_INCLUDED


#include <stdexcept>
#include "DBPoco/Foundation.h"


namespace DBPoco
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
#define DB_POCO_DECLARE_EXCEPTION_CODE(API, CLS, BASE, CODE) \
    class API CLS : public BASE \
    { \
    public: \
        CLS(int code = CODE); \
        CLS(const std::string & msg, int code = CODE); \
        CLS(const std::string & msg, const std::string & arg, int code = CODE); \
        CLS(const std::string & msg, const DBPoco::Exception & exc, int code = CODE); \
        CLS(const CLS & exc); \
        ~CLS() throw(); \
        CLS & operator=(const CLS & exc); \
        const char * name() const throw(); \
        const char * className() const throw(); \
        DBPoco::Exception * clone() const; \
        void rethrow() const; \
    };

#define DB_POCO_DECLARE_EXCEPTION(API, CLS, BASE) DB_POCO_DECLARE_EXCEPTION_CODE(API, CLS, BASE, 0)

#define DB_POCO_IMPLEMENT_EXCEPTION(CLS, BASE, NAME) \
    CLS::CLS(int code) : BASE(code) \
    { \
    } \
    CLS::CLS(const std::string & msg, int code) : BASE(msg, code) \
    { \
    } \
    CLS::CLS(const std::string & msg, const std::string & arg, int code) : BASE(msg, arg, code) \
    { \
    } \
    CLS::CLS(const std::string & msg, const DBPoco::Exception & exc, int code) : BASE(msg, exc, code) \
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
    DBPoco::Exception * CLS::clone() const \
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
DB_POCO_DECLARE_EXCEPTION(Foundation_API, LogicException, Exception)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, AssertionViolationException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, NullPointerException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, NullValueException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, BugcheckException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, InvalidArgumentException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, NotImplementedException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, RangeException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, IllegalStateException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, InvalidAccessException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, SignalException, LogicException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, UnhandledException, LogicException)

DB_POCO_DECLARE_EXCEPTION(Foundation_API, RuntimeException, Exception)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, NotFoundException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, ExistsException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, TimeoutException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, SystemException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, RegularExpressionException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, LibraryLoadException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, LibraryAlreadyLoadedException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, NoThreadAvailableException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, PropertyNotSupportedException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, PoolOverflowException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, NoPermissionException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, OutOfMemoryException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, DataException, RuntimeException)

DB_POCO_DECLARE_EXCEPTION(Foundation_API, DataFormatException, DataException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, SyntaxException, DataException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, CircularReferenceException, DataException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, PathSyntaxException, SyntaxException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, IOException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, ProtocolException, IOException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, FileException, IOException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, FileExistsException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, FileNotFoundException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, PathNotFoundException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, FileReadOnlyException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, FileAccessDeniedException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, CreateFileException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, OpenFileException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, WriteFileException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, ReadFileException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, DirectoryNotEmptyException, FileException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, UnknownURISchemeException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, TooManyURIRedirectsException, RuntimeException)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, URISyntaxException, SyntaxException)

DB_POCO_DECLARE_EXCEPTION(Foundation_API, ApplicationException, Exception)
DB_POCO_DECLARE_EXCEPTION(Foundation_API, BadCastException, RuntimeException)


} // namespace DBPoco


#endif // DB_Foundation_Exception_INCLUDED
