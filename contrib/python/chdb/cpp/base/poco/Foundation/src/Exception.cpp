//
// Exception.cpp
//
// Library: Foundation
// Package: Core
// Module:  Exception
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Exception.h"
#include <typeinfo>


namespace CHDBPoco {


Exception::Exception(int code): _pNested(0), _code(code)
{
}


Exception::Exception(const std::string& msg, int code): _msg(msg), _pNested(0), _code(code)
{
}

Exception::Exception(std::string&& msg, int code): _msg(msg), _pNested(0), _code(code)
{
}


Exception::Exception(const std::string& msg, const std::string& arg, int code): _msg(msg), _pNested(0), _code(code)
{
	if (!arg.empty())
	{
		_msg.append(": ");
		_msg.append(arg);
	}
}


Exception::Exception(const std::string& msg, const Exception& nested, int code): _msg(msg), _pNested(nested.clone()), _code(code)
{
}


Exception::Exception(const Exception& exc):
	std::exception(exc),
	_msg(exc._msg),
	_code(exc._code)
{
	_pNested = exc._pNested ? exc._pNested->clone() : 0;
}

	
Exception::~Exception() throw()
{
	delete _pNested;
}


Exception& Exception::operator = (const Exception& exc)
{
	if (&exc != this)
	{
		Exception* newPNested = exc._pNested ? exc._pNested->clone() : 0;
		delete _pNested;
		_msg     = exc._msg;
		_pNested = newPNested;
		_code    = exc._code;
	}
	return *this;
}


const char* Exception::name() const throw()
{
	return "Exception";
}


const char* Exception::className() const throw()
{
	return typeid(*this).name();
}

	
const char* Exception::what() const throw()
{
	return name();
}

	
std::string Exception::displayText() const
{
	std::string txt = name();
	if (!_msg.empty())
	{
		txt.append(": ");
		txt.append(_msg);
	}
	return txt;
}


void Exception::extendedMessage(const std::string& arg)
{
	if (!arg.empty())
	{
		if (!_msg.empty()) _msg.append(": ");
		_msg.append(arg);
	}
}


Exception* Exception::clone() const
{
	return new Exception(*this);
}


void Exception::rethrow() const
{
	throw *this;
}


CHDB_POCO_IMPLEMENT_EXCEPTION(LogicException, Exception, "Logic exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(AssertionViolationException, LogicException, "Assertion violation")
CHDB_POCO_IMPLEMENT_EXCEPTION(NullPointerException, LogicException, "Null pointer")
CHDB_POCO_IMPLEMENT_EXCEPTION(NullValueException, LogicException, "Null value")
CHDB_POCO_IMPLEMENT_EXCEPTION(BugcheckException, LogicException, "Bugcheck")
CHDB_POCO_IMPLEMENT_EXCEPTION(InvalidArgumentException, LogicException, "Invalid argument")
CHDB_POCO_IMPLEMENT_EXCEPTION(NotImplementedException, LogicException, "Not implemented")
CHDB_POCO_IMPLEMENT_EXCEPTION(RangeException, LogicException, "Out of range")
CHDB_POCO_IMPLEMENT_EXCEPTION(IllegalStateException, LogicException, "Illegal state")
CHDB_POCO_IMPLEMENT_EXCEPTION(InvalidAccessException, LogicException, "Invalid access")
CHDB_POCO_IMPLEMENT_EXCEPTION(SignalException, LogicException, "Signal received")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnhandledException, LogicException, "Unhandled exception")

CHDB_POCO_IMPLEMENT_EXCEPTION(RuntimeException, Exception, "Runtime exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(NotFoundException, RuntimeException, "Not found")
CHDB_POCO_IMPLEMENT_EXCEPTION(ExistsException, RuntimeException, "Exists")
CHDB_POCO_IMPLEMENT_EXCEPTION(TimeoutException, RuntimeException, "Timeout")
CHDB_POCO_IMPLEMENT_EXCEPTION(SystemException, RuntimeException, "System exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(RegularExpressionException, RuntimeException, "Error in regular expression")
CHDB_POCO_IMPLEMENT_EXCEPTION(LibraryLoadException, RuntimeException, "Cannot load library")
CHDB_POCO_IMPLEMENT_EXCEPTION(LibraryAlreadyLoadedException, RuntimeException, "Library already loaded")
CHDB_POCO_IMPLEMENT_EXCEPTION(NoThreadAvailableException, RuntimeException, "No thread available")
CHDB_POCO_IMPLEMENT_EXCEPTION(PropertyNotSupportedException, RuntimeException, "Property not supported")
CHDB_POCO_IMPLEMENT_EXCEPTION(PoolOverflowException, RuntimeException, "Pool overflow")
CHDB_POCO_IMPLEMENT_EXCEPTION(NoPermissionException, RuntimeException, "No permission")
CHDB_POCO_IMPLEMENT_EXCEPTION(OutOfMemoryException, RuntimeException, "Out of memory")
CHDB_POCO_IMPLEMENT_EXCEPTION(DataException, RuntimeException, "Data error")

CHDB_POCO_IMPLEMENT_EXCEPTION(DataFormatException, DataException, "Bad data format")
CHDB_POCO_IMPLEMENT_EXCEPTION(SyntaxException, DataException, "Syntax error")
CHDB_POCO_IMPLEMENT_EXCEPTION(CircularReferenceException, DataException, "Circular reference")
CHDB_POCO_IMPLEMENT_EXCEPTION(PathSyntaxException, SyntaxException, "Bad path syntax")
CHDB_POCO_IMPLEMENT_EXCEPTION(IOException, RuntimeException, "I/O error")
CHDB_POCO_IMPLEMENT_EXCEPTION(ProtocolException, IOException, "Protocol error")
CHDB_POCO_IMPLEMENT_EXCEPTION(FileException, IOException, "File access error")
CHDB_POCO_IMPLEMENT_EXCEPTION(FileExistsException, FileException, "File exists")
CHDB_POCO_IMPLEMENT_EXCEPTION(FileNotFoundException, FileException, "File not found")
CHDB_POCO_IMPLEMENT_EXCEPTION(PathNotFoundException, FileException, "Path not found")
CHDB_POCO_IMPLEMENT_EXCEPTION(FileReadOnlyException, FileException, "File is read-only")
CHDB_POCO_IMPLEMENT_EXCEPTION(FileAccessDeniedException, FileException, "Access to file denied")
CHDB_POCO_IMPLEMENT_EXCEPTION(CreateFileException, FileException, "Cannot create file")
CHDB_POCO_IMPLEMENT_EXCEPTION(OpenFileException, FileException, "Cannot open file")
CHDB_POCO_IMPLEMENT_EXCEPTION(WriteFileException, FileException, "Cannot write file")
CHDB_POCO_IMPLEMENT_EXCEPTION(ReadFileException, FileException, "Cannot read file")
CHDB_POCO_IMPLEMENT_EXCEPTION(DirectoryNotEmptyException, FileException, "Directory not empty")
CHDB_POCO_IMPLEMENT_EXCEPTION(UnknownURISchemeException, RuntimeException, "Unknown URI scheme")
CHDB_POCO_IMPLEMENT_EXCEPTION(TooManyURIRedirectsException, RuntimeException, "Too many URI redirects")
CHDB_POCO_IMPLEMENT_EXCEPTION(URISyntaxException, SyntaxException, "Bad URI syntax")

CHDB_POCO_IMPLEMENT_EXCEPTION(ApplicationException, Exception, "Application exception")
CHDB_POCO_IMPLEMENT_EXCEPTION(BadCastException, RuntimeException, "Bad cast exception")


} // namespace CHDBPoco
