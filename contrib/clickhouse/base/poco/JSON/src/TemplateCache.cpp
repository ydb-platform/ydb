//
// TemplateCache.cpp
//
// Library: JSON
// Package: JSON
// Module:  TemplateCache
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "DBPoco/File.h"
#include "DBPoco/JSON/TemplateCache.h"


namespace DBPoco {
namespace JSON {


TemplateCache* TemplateCache::_pInstance = 0;


TemplateCache::TemplateCache(): _pLogger(0)
{
	setup();
}


TemplateCache::~TemplateCache()
{
	_pInstance = 0;
}


void TemplateCache::setup()
{
	DB_poco_assert (_pInstance == 0);
	_pInstance = this;
}


Template::Ptr TemplateCache::getTemplate(const Path& path)
{
	if (_pLogger)
	{
		DB_poco_trace_f1(*_pLogger, "Trying to load %s", path.toString());
	}

	Path templatePath = resolvePath(path);
	std::string templatePathname = templatePath.toString();
	
	if (_pLogger)
	{
		DB_poco_trace_f1(*_pLogger, "Path resolved to %s", templatePathname);
	}
	
	File templateFile(templatePathname);

	Template::Ptr tpl;

	std::map<std::string, Template::Ptr>::iterator it = _cache.find(templatePathname);
	if (it == _cache.end())
	{
		if (templateFile.exists())
		{
			if (_pLogger)
			{
				DB_poco_information_f1(*_pLogger, "Loading template %s", templatePath.toString());
			}

			tpl = new Template(templatePath);

			try
			{
				tpl->parse();
				_cache[templatePathname] = tpl;
			}
			catch (JSONTemplateException& jte)
			{
				if (_pLogger)
				{
					DB_poco_error_f2(*_pLogger, "Template %s contains an error: %s", templatePath.toString(), jte.message());
				}
			}
		}
		else
		{
			if (_pLogger)
			{
				DB_poco_error_f1(*_pLogger, "Template file %s doesn't exist", templatePath.toString());
			}
			throw FileNotFoundException(templatePathname);
		}
	}
	else
	{
		tpl = it->second;
		if (tpl->parseTime() < templateFile.getLastModified())
		{
			if (_pLogger)
			{
				DB_poco_information_f1(*_pLogger, "Reloading template %s", templatePath.toString());
			}

			tpl = new Template(templatePath);

			try
			{
				tpl->parse();
				_cache[templatePathname] = tpl;
			}
			catch (JSONTemplateException& jte)
			{
				if (_pLogger)
				{
					DB_poco_error_f2(*_pLogger, "Template %s contains an error: %s", templatePath.toString(), jte.message());
				}
			}
		}
	}

	return tpl;
}


Path TemplateCache::resolvePath(const Path& path) const
{
	if (path.isAbsolute())
		return path;

	for (std::vector<Path>::const_iterator it = _includePaths.begin(); it != _includePaths.end(); ++it)
	{
		Path templatePath(*it, path);

		File templateFile(templatePath);
		if (templateFile.exists())
		{
			if (_pLogger)
			{
				DB_poco_trace_f2(*_pLogger, "%s template file resolved to %s", path.toString(), templatePath.toString());
			}
			return templatePath;
		}
		if (_pLogger)
		{
			DB_poco_trace_f1(*_pLogger, "%s doesn't exist", templatePath.toString());
		}
	}

	throw FileNotFoundException(path.toString());
}


} } // DBPoco::JSON
