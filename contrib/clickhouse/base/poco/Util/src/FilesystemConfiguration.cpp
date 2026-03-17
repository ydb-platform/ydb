//
// FilesystemConfiguration.cpp
//
// Library: Util
// Package: Configuration
// Module:  FilesystemConfiguration
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/Util/FilesystemConfiguration.h"
#include "DBPoco/File.h"
#include "DBPoco/Path.h"
#include "DBPoco/DirectoryIterator.h"
#include "DBPoco/StringTokenizer.h"
#include "DBPoco/FileStream.h"


using DBPoco::Path;
using DBPoco::File;
using DBPoco::DirectoryIterator;
using DBPoco::StringTokenizer;


namespace DBPoco {
namespace Util {


FilesystemConfiguration::FilesystemConfiguration(const std::string& path):
	_path(path)
{
	_path.makeDirectory();
}


FilesystemConfiguration::~FilesystemConfiguration()
{
}


void FilesystemConfiguration::clear()
{
	File regDir(_path);
	regDir.remove(true);
}


bool FilesystemConfiguration::getRaw(const std::string& key, std::string& value) const
{
	Path p(keyToPath(key));
	p.setFileName("data");
	File f(p);
	if (f.exists())
	{
		value.reserve((std::string::size_type) f.getSize());
		DBPoco::FileInputStream istr(p.toString());
		int c = istr.get();
		while (c != std::char_traits<char>::eof())
		{
			value += (char) c;
			c = istr.get();
		}
		return true;
	}
	else return false;
}


void FilesystemConfiguration::setRaw(const std::string& key, const std::string& value)
{
	Path p(keyToPath(key));
	File dir(p);
	dir.createDirectories();
	p.setFileName("data");
	DBPoco::FileOutputStream ostr(p.toString());
	ostr.write(value.data(), (std::streamsize) value.length());
}


void FilesystemConfiguration::enumerate(const std::string& key, Keys& range) const
{
	Path p(keyToPath(key));
	File dir(p);
	if (!dir.exists())
	{
		return;
	}

	DirectoryIterator it(p);
	DirectoryIterator end;
	while (it != end)
	{
		 if (it->isDirectory())
			range.push_back(it.name());
		++it;
	}
}


void FilesystemConfiguration::removeRaw(const std::string& key)
{
	Path p(keyToPath(key));
	File dir(p);
	if (dir.exists())
	{
		dir.remove(true);
	}
}


Path FilesystemConfiguration::keyToPath(const std::string& key) const
{
	Path result(_path);
	StringTokenizer tokenizer(key, ".", StringTokenizer::TOK_IGNORE_EMPTY | StringTokenizer::TOK_TRIM);
	for (StringTokenizer::Iterator it = tokenizer.begin(); it != tokenizer.end(); ++it)
	{
		result.pushDirectory(*it);
	}	
	return result;
}


} } // namespace DBPoco::Util
