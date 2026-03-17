//
// FilePartSource.cpp
//
// Library: Net
// Package: Messages
// Module:  FilePartSource
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Net/FilePartSource.h"
#include "DBPoco/Path.h"
#include "DBPoco/File.h"
#include "DBPoco/Exception.h"


using DBPoco::Path;
using DBPoco::OpenFileException;


namespace DBPoco {
namespace Net {


FilePartSource::FilePartSource(const std::string& path):
	_path(path), _istr(path)
{
	Path p(path);
	_filename = p.getFileName();
	if (!_istr.good())
		throw OpenFileException(path);
}


FilePartSource::FilePartSource(const std::string& path, const std::string& mediaType):
	PartSource(mediaType),
	_path(path),
	_istr(path)
{
	Path p(path);
	_filename = p.getFileName();
	if (!_istr.good())
		throw OpenFileException(path);
}


FilePartSource::FilePartSource(const std::string& path, const std::string& filename, const std::string& mediaType):
	PartSource(mediaType),
	_path(path),
	_filename(filename),
	_istr(path)
{
	Path p(path);
	if (!_istr.good())
		throw OpenFileException(path);
}


FilePartSource::~FilePartSource()
{
}


std::istream& FilePartSource::stream()
{
	return _istr;
}


const std::string& FilePartSource::filename() const
{
	return _filename;
}


std::streamsize FilePartSource::getContentLength() const
{
	DBPoco::File p(_path);
	return static_cast<std::streamsize>(p.getSize());
}


} } // namespace DBPoco::Net
