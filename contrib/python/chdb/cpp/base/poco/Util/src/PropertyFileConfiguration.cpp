//
// PropertyFileConfiguration.cpp
//
// Library: Util
// Package: Configuration
// Module:  PropertyFileConfiguration
//
// Copyright (c) 2004-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "CHDBPoco/Util/PropertyFileConfiguration.h"
#include "CHDBPoco/Exception.h"
#include "CHDBPoco/String.h"
#include "CHDBPoco/Path.h"
#include "CHDBPoco/FileStream.h"
#include "CHDBPoco/LineEndingConverter.h"
#include "CHDBPoco/Ascii.h"


using CHDBPoco::trim;
using CHDBPoco::Path;


namespace CHDBPoco {
namespace Util {


PropertyFileConfiguration::PropertyFileConfiguration()
{
}


PropertyFileConfiguration::PropertyFileConfiguration(std::istream& istr)
{
	load(istr);
}

	
PropertyFileConfiguration::PropertyFileConfiguration(const std::string& path)
{
	load(path);
}


PropertyFileConfiguration::~PropertyFileConfiguration()
{
}

	
void PropertyFileConfiguration::load(std::istream& istr)
{
	clear();
	while (!istr.eof())
	{
		parseLine(istr);
	}
}

	
void PropertyFileConfiguration::load(const std::string& path)
{
	CHDBPoco::FileInputStream istr(path);
	if (istr.good())
		load(istr);
	else
		throw CHDBPoco::OpenFileException(path);
}


void PropertyFileConfiguration::save(std::ostream& ostr) const
{
	MapConfiguration::iterator it = begin();
	MapConfiguration::iterator ed = end();
	while (it != ed)
	{
		ostr << it->first << ": ";
		for (std::string::const_iterator its = it->second.begin(); its != it->second.end(); ++its)
		{
			switch (*its)
			{
			case '\t':
				ostr << "\\t";
				break;
			case '\r':
				ostr << "\\r";
				break;
			case '\n':
				ostr << "\\n";
				break;
			case '\f':
				ostr << "\\f";
				break;
			case '\\':
				ostr << "\\\\";
				break;
			default:
				ostr << *its;
				break;
			}
		}
		ostr << "\n";
		++it;
	}
}


void PropertyFileConfiguration::save(const std::string& path) const
{
	CHDBPoco::FileOutputStream ostr(path);
	if (ostr.good())
	{
		CHDBPoco::OutputLineEndingConverter lec(ostr);
		save(lec);
		lec.flush();
		ostr.flush();
		if (!ostr.good()) throw CHDBPoco::WriteFileException(path);
	}
	else throw CHDBPoco::CreateFileException(path);
}


void PropertyFileConfiguration::parseLine(std::istream& istr)
{
	static const int eof = std::char_traits<char>::eof(); 

	int c = istr.get();
	while (c != eof && CHDBPoco::Ascii::isSpace(c)) c = istr.get();
	if (c != eof)
	{
		if (c == '#' || c == '!')
		{
			while (c != eof && c != '\n' && c != '\r') c = istr.get();
		}
		else
		{
			std::string key;
			while (c != eof && c != '=' && c != ':' && c != '\r' && c != '\n') { key += (char) c; c = istr.get(); }
			std::string value;
			if (c == '=' || c == ':')
			{
				c = readChar(istr);
				while (c != eof && c) { value += (char) c; c = readChar(istr); }
			}
			setRaw(trim(key), trim(value));
		}
	}
}


int PropertyFileConfiguration::readChar(std::istream& istr)
{
	for (;;)
	{
		int c = istr.get();
		if (c == '\\')
		{
			c = istr.get();
			switch (c)
			{
			case 't':
				return '\t';
			case 'r':
				return '\r';
			case 'n':
				return '\n';
			case 'f':
				return '\f';
			case '\r':
				if (istr.peek() == '\n')
					istr.get();
				continue;
			case '\n':
				continue;
			default:
				return c;
			}
		}
		else if (c == '\n' || c == '\r')
			return 0;
		else
			return c;
	}
}


} } // namespace CHDBPoco::Util
