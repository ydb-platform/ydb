#pragma once

#include "StoragePython.h"
#include "PybindWrapper.h"

#include "clickhouse_config.h"
#include <Storages/ColumnsDescription.h>
#include <TableFunctions/ITableFunction.h>
#include <CHDBPoco/Logger.h>

namespace DB_CHDB
{

class TableFunctionFactory;
void registerTableFunctionPython(TableFunctionFactory & factory);

class TableFunctionPython : public ITableFunction
{
public:
    static constexpr auto name = "python";
    std::string getName() const override { return name; }
    ~TableFunctionPython() override
    {
        // Acquire the GIL before destroying the reader object
        py::gil_scoped_acquire acquire;
        reader.dec_ref();
        reader.release();
    }

private:
    CHDBPoco::Logger * logger = &CHDBPoco::Logger::get("TableFunctionPython");
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "Python"; }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    py::object reader;
};

}
