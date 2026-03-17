#include "PythonImportCache.h"
#include "PythonImporter.h"

#include <Common/Exception.h>
#include <stack>

namespace DB_CHDB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

}

namespace CHDB {

py::handle PythonImportCacheItem::operator()(bool load) {
	if (IsLoaded())
		return object;

	std::stack<PythonImportCacheItem *> hierarchy;

	PythonImportCacheItem * item = this;
	while (item)
	{
		hierarchy.emplace(item);
		item = item->parent;
	}

	return PythonImporter::Import(hierarchy, load);
}

bool PythonImportCacheItem::LoadSucceeded() const
{
	return load_succeeded;
}

bool PythonImportCacheItem::IsLoaded() const
{
	return object.ptr() != nullptr;
}

py::handle PythonImportCacheItem::AddCache(PythonImportCache & cache, py::object object)
{
	return cache.AddCache(std::move(object));
}

void PythonImportCacheItem::LoadModule(PythonImportCache & cache)
{
	try
	{
		py::gil_assert();
		object = AddCache(cache, std::move(py::module::import(name.c_str())));
		load_succeeded = true;
	}
	catch (py::error_already_set &e)
	{
		if (IsRequired())
		{
			throw DB_CHDB::Exception(DB_CHDB::ErrorCodes::LOGICAL_ERROR,
			    				"Required module {} failed to import, due to the following Python exception:\n {}", name, e.what());
		}
		object = nullptr;
		return;
	}
}

void PythonImportCacheItem::LoadAttribute(PythonImportCache & cache, py::handle source)
{
	if (py::hasattr(source, name.c_str()))
		object = AddCache(cache, std::move(source.attr(name.c_str())));
	else
		object = nullptr;
}

py::handle PythonImportCacheItem::Load(PythonImportCache & cache, py::handle source, bool load)
{
	if (IsLoaded())
		return object;

	if (!load)
		return object;

	if (is_module)
		LoadModule(cache);
	else
		LoadAttribute(cache, source);

	return object;
}

PythonImportCache::~PythonImportCache()
{
	try
	{
		py::gil_scoped_acquire acquire;
		owned_objects.clear();
	}
	catch (...)
	{
	}
}

py::handle PythonImportCache::AddCache(py::object item)
{
	auto object_ptr = item.ptr();
	owned_objects.push_back(std::move(item));
	return object_ptr;
}

} // namespace CHDB
