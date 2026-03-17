
#ifndef _GETDATA_H_
#define _GETDATA_H_

void GetData_init();

PyObject* PythonTypeFromSqlType(Cursor* cur, SQLSMALLINT type);

PyObject* GetData(Cursor* cur, Py_ssize_t iCol);

/**
 * If this sql type has a user-defined conversion, the index into the connection's `conv_funcs` array is returned.
 * Otherwise -1 is returned.
 */
int GetUserConvIndex(Cursor* cur, SQLSMALLINT sql_type);

#endif // _GETDATA_H_
