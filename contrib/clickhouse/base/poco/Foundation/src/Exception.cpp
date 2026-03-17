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


#include "DBPoco/Exception.h"
#include <typeinfo>


namespace DBPoco {


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


DB_POCO_IMPLEMENT_EXCEPTION(LogicException, Exception, "Logic exception")
DB_POCO_IMPLEMENT_EXCEPTION(AssertionViolationException, LogicException, "Assertion violation")
DB_POCO_IMPLEMENT_EXCEPTION(NullPointerException, LogicException, "Null pointer")
DB_POCO_IMPLEMENT_EXCEPTION(NullValueException, LogicException, "Null value")
DB_POCO_IMPLEMENT_EXCEPTION(BugcheckException, LogicException, "Bugcheck")
DB_POCO_IMPLEMENT_EXCEPTION(InvalidArgumentException, LogicException, "Invalid argument")
DB_POCO_IMPLEMENT_EXCEPTION(NotImplementedException, LogicException, "Not implemented")
DB_POCO_IMPLEMENT_EXCEPTION(RangeException, LogicException, "Out of range")
DB_POCO_IMPLEMENT_EXCEPTION(IllegalStateException, LogicException, "Illegal state")
DB_POCO_IMPLEMENT_EXCEPTION(InvalidAccessException, LogicException, "Invalid access")
DB_POCO_IMPLEMENT_EXCEPTION(SignalException, LogicException, "Signal received")
DB_POCO_IMPLEMENT_EXCEPTION(UnhandledException, LogicException, "Unhandled exception")

DB_POCO_IMPLEMENT_EXCEPTION(RuntimeException, Exception, "Runtime exception")
DB_POCO_IMPLEMENT_EXCEPTION(NotFoundException, RuntimeException, "Not found")
DB_POCO_IMPLEMENT_EXCEPTION(ExistsException, RuntimeException, "Exists")
DB_POCO_IMPLEMENT_EXCEPTION(TimeoutException, RuntimeException, "Timeout")
DB_POCO_IMPLEMENT_EXCEPTION(SystemException, RuntimeException, "System exception")
DB_POCO_IMPLEMENT_EXCEPTION(RegularExpressionException, RuntimeException, "Error in regular expression")
DB_POCO_IMPLEMENT_EXCEPTION(LibraryLoadException, RuntimeException, "Cannot load library")
DB_POCO_IMPLEMENT_EXCEPTION(LibraryAlreadyLoadedException, RuntimeException, "Library already loaded")
DB_POCO_IMPLEMENT_EXCEPTION(NoThreadAvailableException, RuntimeException, "No thread available")
DB_POCO_IMPLEMENT_EXCEPTION(PropertyNotSupportedException, RuntimeException, "Property not supported")
DB_POCO_IMPLEMENT_EXCEPTION(PoolOverflowException, RuntimeException, "Pool overflow")
DB_POCO_IMPLEMENT_EXCEPTION(NoPermissionException, RuntimeException, "No permission")
DB_POCO_IMPLEMENT_EXCEPTION(OutOfMemoryException, RuntimeException, "Out of memory")
DB_POCO_IMPLEMENT_EXCEPTION(DataException, RuntimeException, "Data error")

DB_POCO_IMPLEMENT_EXCEPTION(DataFormatException, DataException, "Bad data format")
DB_POCO_IMPLEMENT_EXCEPTION(SyntaxException, DataException, "Syntax error")
DB_POCO_IMPLEMENT_EXCEPTION(CircularReferenceException, DataException, "Circular reference")
DB_POCO_IMPLEMENT_EXCEPTION(PathSyntaxException, SyntaxException, "Bad path syntax")
DB_POCO_IMPLEMENT_EXCEPTION(IOException, RuntimeException, "I/O error")
DB_POCO_IMPLEMENT_EXCEPTION(ProtocolException, IOException, "Protocol error")
DB_POCO_IMPLEMENT_EXCEPTION(FileException, IOException, "File access error")
DB_POCO_IMPLEMENT_EXCEPTION(FileExistsException, FileException, "File exists")
DB_POCO_IMPLEMENT_EXCEPTION(FileNotFoundException, FileException, "File not found")
DB_POCO_IMPLEMENT_EXCEPTION(PathNotFoundException, FileException, "Path not found")
DB_POCO_IMPLEMENT_EXCEPTION(FileReadOnlyException, FileException, "File is read-only")
DB_POCO_IMPLEMENT_EXCEPTION(FileAccessDeniedException, FileException, "Access to file denied")
DB_POCO_IMPLEMENT_EXCEPTION(CreateFileException, FileException, "Cannot create file")
DB_POCO_IMPLEMENT_EXCEPTION(OpenFileException, FileException, "Cannot open file")
DB_POCO_IMPLEMENT_EXCEPTION(WriteFileException, FileException, "Cannot write file")
DB_POCO_IMPLEMENT_EXCEPTION(ReadFileException, FileException, "Cannot read file")
DB_POCO_IMPLEMENT_EXCEPTION(DirectoryNotEmptyException, FileException, "Directory not empty")
DB_POCO_IMPLEMENT_EXCEPTION(UnknownURISchemeException, RuntimeException, "Unknown URI scheme")
DB_POCO_IMPLEMENT_EXCEPTION(TooManyURIRedirectsException, RuntimeException, "Too many URI redirects")
DB_POCO_IMPLEMENT_EXCEPTION(URISyntaxException, SyntaxException, "Bad URI syntax")

DB_POCO_IMPLEMENT_EXCEPTION(ApplicationException, Exception, "Application exception")
DB_POCO_IMPLEMENT_EXCEPTION(BadCastException, RuntimeException, "Bad cast exception")


} // namespace DBPoco
