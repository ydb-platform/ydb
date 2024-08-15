#ifndef YQL_READ_TABLE_H
#define YQL_READ_TABLE_H

struct yql_table_iterator;
extern struct yql_table_iterator* yql_read_table(const char* name, int num_columns, const char* columns[]);
extern const char* yql_iterator_error(struct yql_table_iterator* iterator);
extern bool yql_iterator_has_data(struct yql_table_iterator* iterator);
extern bool yql_iterator_value(struct yql_table_iterator* iterator, int column_index, const char** value);
extern void yql_iterator_move(struct yql_table_iterator* iterator);
extern void yql_iterator_close(struct yql_table_iterator** iterator);

#endif /* YQL_READ_TABLE_H */

