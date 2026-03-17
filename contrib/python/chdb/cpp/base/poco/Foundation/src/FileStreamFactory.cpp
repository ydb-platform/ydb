//
// FileStreamFactory.cpp
//
// Library: Foundation
// Package: URI
// Module:  FileStreamFactory
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/FileStreamFactory.h"
#include "CHDBPoco/URI.h"
#include "CHDBPoco/Path.h"
#include "CHDBPoco/File.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/FileStream.h"


namespace CHDBPoco {


FileStreamFactory::FileStreamFactory()
{
}


FileStreamFactory::~FileStreamFactory()
{
}


std::istream* FileStreamFactory::open(const URI& uri)
{
	CHDB_poco_assert (uri.isRelative() || uri.getScheme() == "file");

	std::string uriPath = uri.getPath();
	if (uriPath.substr(0, 2) == "./")
		uriPath.erase(0, 2);
	Path p(uriPath, Path::PATH_UNIX);
	p.setNode(uri.getHost());
	return open(p);
}


std::istream* FileStreamFactory::open(const Path& path)
{
	File file(path);
	if (!file.exists()) throw FileNotFoundException(path.toString());
	
	FileInputStream* istr = new FileInputStream(path.toString(), std::ios::binary);
	if (!istr->good())
	{
		delete istr;
		throw OpenFileException(path.toString());
	}	
	return istr;
}


} // namespace CHDBPoco
