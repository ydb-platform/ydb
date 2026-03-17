#pragma once

#include "PythonImportCacheItem.h"

namespace CHDB {

struct DecimalCacheItem : public PythonImportCacheItem
{
public:
	static constexpr const char * Name = "decimal";

	DecimalCacheItem() : PythonImportCacheItem("decimal"), Decimal("Decimal", this)
	{
	}

	~DecimalCacheItem() override = default;

	PythonImportCacheItem Decimal;
};

} // namespace CHDB
