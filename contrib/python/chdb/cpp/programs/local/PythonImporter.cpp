#include "PythonImporter.h"
#include "PythonImportCacheItem.h"

namespace CHDB {

PythonImportCachePtr PythonImporter::python_import_cache = nullptr;

py::handle PythonImporter::Import(std::stack<PythonImportCacheItem *> & hierarchy, bool load) {
	auto & import_cache = ImportCache();
	py::handle source(nullptr);

	while (!hierarchy.empty()) {
		// From top to bottom, import them
		auto item = hierarchy.top();
		hierarchy.pop();
		source = item->Load(import_cache, source, load);
		if (!source)
		{
			// If load is false, or the module load fails and is not required, we return early
			break;
		}
	}

	return source;
}

PythonImportCache & PythonImporter::ImportCache()
{
	if (!python_import_cache)
		python_import_cache = std::make_shared<PythonImportCache>();

	return *python_import_cache.get();
}

void PythonImporter::destroy()
{
	python_import_cache.reset();
}

} // namespace CHDB
