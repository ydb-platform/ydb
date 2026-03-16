#pragma once

#include "PythonImportCacheItem.h"

namespace CHDB {

struct DatetimeDatetimeCacheItem : public PythonImportCacheItem
{

public:
	DatetimeDatetimeCacheItem(PythonImportCacheItem * parent)
	    : PythonImportCacheItem("datetime", parent), min("min", this), max("max", this), combine("combine", this)
	{
	}

	~DatetimeDatetimeCacheItem() override = default;

	PythonImportCacheItem min;
	PythonImportCacheItem max;
	PythonImportCacheItem combine;
};

struct DatetimeDateCacheItem : public PythonImportCacheItem
{

public:
	DatetimeDateCacheItem(PythonImportCacheItem * parent)
	    : PythonImportCacheItem("date", parent), max("max", this), min("min", this)
	{
	}

	~DatetimeDateCacheItem() override = default;

	PythonImportCacheItem max;
	PythonImportCacheItem min;
};

struct DatetimeCacheItem : public PythonImportCacheItem
{

public:
	static constexpr const char *Name = "datetime";

public:
	DatetimeCacheItem()
	    : PythonImportCacheItem("datetime"), date(this), time("time", this), timedelta("timedelta", this),
	      datetime(this), timezone("timezone", this)
	{
	}

	~DatetimeCacheItem() override = default;

	DatetimeDateCacheItem date;
	PythonImportCacheItem time;
	PythonImportCacheItem timedelta;
	DatetimeDatetimeCacheItem datetime;
	PythonImportCacheItem timezone;
};

} // namespace CHDB
