#include "pg_compat.h"

extern "C" {
#include "postgres.h"
#include "yql/read_table.h"
}

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

struct yql_table_iterator {
    const TVector<TMaybe<TString>>* Data;
    char* Error;
    size_t NumColumns;
    size_t* ColumnsRemap;
    size_t Pos;
    size_t RowStep;
};

extern "C" struct yql_table_iterator* yql_read_table(const char* name, int num_columns, const char* columns[]) {
    yql_table_iterator* res = (yql_table_iterator*)palloc(sizeof(yql_table_iterator));
    Zero(*res);
    if (!name || num_columns < 0 || !columns) {
        res->Error = pstrdup("bad arguments");
        return res;
    }

    TString fullName = name;
    auto pos = fullName.find('.');
    if (pos == TString::npos) {
        res->Error = pstrdup("expected full table name");
        return res;
    }

    auto schema = fullName.substr(0, pos);
    auto table = fullName.substr(pos + 1);

    res->NumColumns = num_columns;
    res->Pos = 0;
    if (num_columns) {
        res->ColumnsRemap = (size_t*)palloc(sizeof(size_t) * res->NumColumns);
    }

    TVector<TString> columnNames(res->NumColumns);
    for (size_t i = 0; i < res->NumColumns; ++i) {
        columnNames[i] = columns[i];
    }

    try {
        res->Data = NYql::NPg::ReadTable(NYql::NPg::TTableInfoKey{schema, table}, columnNames, res->ColumnsRemap, res->RowStep);
    } catch (yexception& e) {
        res->Error = pstrdup(e.what());
    }

    return res;
}

extern "C" const char* yql_iterator_error(struct yql_table_iterator* iterator) {
    if (!iterator) {
        return nullptr;
    }

    return iterator->Error;
}

extern "C" bool yql_iterator_has_data(struct yql_table_iterator* iterator) {
    if (!iterator || !iterator->Data) {
        return false;
    }

    return iterator->Pos < iterator->Data->size();
}

extern "C" bool yql_iterator_value(struct yql_table_iterator* iterator, int column_index, const char** value) {
    if (!iterator || !iterator->Data || !value) {
        return false;
    }

    if (column_index < 0 || column_index >= iterator->NumColumns) {
        return false;
    }

    const auto& cell = (*iterator->Data)[iterator->Pos + iterator->ColumnsRemap[column_index]];
    if (!cell) {
        *value = nullptr;
        return true;
    }

    *value = cell.GetRef().c_str();
    return true;
}

extern "C" void yql_iterator_move(struct yql_table_iterator* iterator) {
    if (!iterator || !iterator->Data) {
        return;
    }

    if (iterator->Pos < iterator->Data->size()) {
        iterator->Pos += iterator->RowStep;
    }
}

extern "C" void yql_iterator_close(struct yql_table_iterator** iterator) {
    if (!iterator || !*iterator) {
        return;
    }

    if ((*iterator)->ColumnsRemap) {
        pfree((*iterator)->ColumnsRemap);
    }

    if ((*iterator)->Error) {
        pfree((*iterator)->Error);
    }

    pfree(*iterator);
    *iterator = nullptr;
}
