
#ifndef PARAMS_H
#define PARAMS_H

bool Params_init();

struct Cursor;

bool PrepareAndBind(Cursor* cur, PyObject* pSql, PyObject* params, bool skip_first);
bool ExecuteMulti(Cursor* cur, PyObject* pSql, PyObject* paramArrayObj);
bool GetParameterInfo(Cursor* cur, Py_ssize_t index, PyObject* param, ParamInfo& info, bool isTVP);
void FreeParameterData(Cursor* cur);
void FreeParameterInfo(Cursor* cur);

#endif
