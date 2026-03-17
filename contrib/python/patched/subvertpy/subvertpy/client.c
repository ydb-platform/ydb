/*
 * Copyright © 2008 Jelmer Vernooij <jelmer@jelmer.uk>
 * -*- coding: utf-8 -*-
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA
 */
#include <stdbool.h>
#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <structmember.h>
#include <apr_general.h>
#include <svn_opt.h>
#include <svn_client.h>
#include <svn_config.h>
#include <svn_path.h>

#include "util.h"
#include "ra.h"
#include "wc.h"

#if ONLY_SINCE_SVN(1, 6)
#define INFO_SIZE size64
#define WORKING_SIZE working_size64
#else
#define INFO_SIZE size
#define WORKING_SIZE working_size
#endif

extern PyTypeObject Client_Type;
extern PyTypeObject Config_Type;
extern PyTypeObject ConfigItem_Type;
extern PyTypeObject Info_Type;
extern PyTypeObject WCInfo_Type;

typedef struct {
    PyObject_VAR_HEAD
    svn_config_t *item;
    PyObject *parent;
} ConfigItemObject;

typedef struct {
    PyObject_VAR_HEAD
#if ONLY_SINCE_SVN(1, 7)
    svn_wc_info_t info;
#else
    svn_info_t info;
#endif
    apr_pool_t *pool;
} WCInfoObject;

typedef struct {
    PyObject_VAR_HEAD
#if ONLY_SINCE_SVN(1, 7)
    svn_client_info2_t info;
#else
    svn_info_t info;
#endif
    WCInfoObject *wc_info;
    apr_pool_t *pool;
} InfoObject;

static int client_set_auth(PyObject *self, PyObject *auth, void *closure);
static int client_set_config(PyObject *self, PyObject *auth, void *closure);

static bool client_check_path(const char *path, apr_pool_t *scratch_pool)
{
    return svn_path_is_canonical(path, scratch_pool);
}

static bool client_path_list_to_apr_array(apr_pool_t *pool, PyObject *l, apr_array_header_t **ret)
{
    int i;
    const char *path;
    if (l == Py_None) {
        *ret = NULL;
        return true;
    }
    if (PyUnicode_Check(l) || PyBytes_Check(l)) {
        *ret = apr_array_make(pool, 1, sizeof(char *));
        path = py_object_to_svn_string(l, pool);
        if (path == NULL) {
            return false;
        }
        if (!client_check_path(path, pool)) {
            PyErr_SetString(PyExc_ValueError, "Expected canonical path or URL");
            return false;
        }
        APR_ARRAY_PUSH(*ret, const char *) = path;
    } else if (PyList_Check(l)) {
        *ret = apr_array_make(pool, PyList_Size(l), sizeof(char *));
        for (i = 0; i < PyList_GET_SIZE(l); i++) {
            PyObject *item = PyList_GET_ITEM(l, i);
            path = py_object_to_svn_string(item, pool);
            if (!client_check_path(path, pool)) {
                PyErr_SetString(PyExc_ValueError, "Expected canonical path or URL");
                return false;
            }
            APR_ARRAY_PUSH(*ret, const char *) = path;
        }
    } else {
        PyErr_Format(PyExc_TypeError, "Expected list of strings, got: %s",
                     l->ob_type->tp_name);
        return false;
    }

    return true;
}

static bool to_opt_revision(PyObject *arg, svn_opt_revision_t *ret)
{
    if (PyLong_Check(arg)) {
        ret->kind = svn_opt_revision_number;
        ret->value.number = PyLong_AsLong(arg);
        if (ret->value.number == -1 && PyErr_Occurred()) {
            return false;
        }
        return true;
#if PY_MAJOR_VERSION < 3
    } else if (PyInt_Check(arg)) {
        ret->kind = svn_opt_revision_number;
        ret->value.number = PyInt_AsLong(arg);
        if (ret->value.number == -1 && PyErr_Occurred()) {
            return false;
        }
        return true;
#endif
    } else if (arg == Py_None) {
        ret->kind = svn_opt_revision_unspecified;
        return true;
    } else if (PyUnicode_Check(arg) || PyBytes_Check(arg)) {
        char *text;
        if (PyUnicode_Check(arg)) {
            arg = PyUnicode_AsUTF8String(arg);
        } else {
            Py_INCREF(arg);
        }

        text = PyBytes_AsString(arg);
        if (!strcmp(text, "HEAD")) {
            ret->kind = svn_opt_revision_head;
            Py_DECREF(arg);
            return true;
        } else if (!strcmp(text, "WORKING")) {
            ret->kind = svn_opt_revision_working;
            Py_DECREF(arg);
            return true;
        } else if (!strcmp(text, "BASE")) {
            ret->kind = svn_opt_revision_base;
            Py_DECREF(arg);
            return true;
        }
        Py_DECREF(arg);
    }

    PyErr_SetString(PyExc_ValueError, "Unable to parse revision");
    return false;
}

static PyObject *wrap_py_commit_items(const apr_array_header_t *commit_items)
{
    PyObject *ret;
    int i;

    ret = PyList_New(commit_items->nelts);
    if (ret == NULL)
        return NULL;

    assert(commit_items->elt_size == sizeof(svn_client_commit_item2_t *));

    for (i = 0; i < commit_items->nelts; i++) {
        svn_client_commit_item2_t *commit_item =
            APR_ARRAY_IDX(commit_items, i, svn_client_commit_item2_t *);
        PyObject *item, *copyfrom;

        if (commit_item->copyfrom_url != NULL) {
            copyfrom = Py_BuildValue("(sl)", commit_item->copyfrom_url,
                                     commit_item->copyfrom_rev);
            if (copyfrom == NULL) {
                Py_DECREF(ret);
                return NULL;
            }
        } else {
            copyfrom = Py_None;
            Py_INCREF(copyfrom);
        }

        item = Py_BuildValue("(szlNi)",
                             /* commit_item->path */ "foo",
                             commit_item->url, commit_item->revision,
                             copyfrom,
                             commit_item->state_flags);
        if (item == NULL) {
            Py_DECREF(ret);
            return NULL;
        }

        if (PyList_SetItem(ret, i, item) != 0) {
            Py_DECREF(ret);
            return NULL;
        }
    }

    return ret;
}

#if ONLY_SINCE_SVN(1, 8)
static svn_error_t *proplist_receiver2(
    void *prop_list, const char *path, apr_hash_t *prop_hash,
    apr_array_header_t *inherited_props, apr_pool_t *scratch_pool)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *prop_dict;
    PyObject *value;

    prop_dict = prop_hash_to_dict(prop_hash);

    if (prop_dict == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    value = Py_BuildValue("(sO)", path, prop_dict);
    if (value == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    /* TODO(jelmer): Convert inherited_props */

    if (PyList_Append(prop_list, value) != 0) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    PyGILState_Release(state);

    return NULL;
}
#elif ONLY_SINCE_SVN(1, 5)
static svn_error_t *proplist_receiver(void *prop_list, const char *path,
                                      apr_hash_t *prop_hash, apr_pool_t *pool)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *prop_dict;
    PyObject *value;

    prop_dict = prop_hash_to_dict(prop_hash);

    if (prop_dict == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    value = Py_BuildValue("(sO)", path, prop_dict);
    if (value == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    if (PyList_Append(prop_list, value) != 0) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    PyGILState_Release(state);

    return NULL;
}
#endif

#if ONLY_SINCE_SVN(1, 8)
static svn_error_t *list_receiver2(void *dict, const char *path,
                                  const svn_dirent_t *dirent,
                                  const svn_lock_t *lock, const char *abs_path,
                                  const char *external_parent_url,
                                  const char *external_target,
                                  apr_pool_t *pool)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *value;

    value = py_dirent(dirent, SVN_DIRENT_ALL);
    if (value == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    if (external_parent_url != NULL || external_target != NULL) {
        value = Py_BuildValue("(Nzz)", value, external_parent_url, external_target);
    }

    if (PyDict_SetItemString(dict, path, value) != 0) {
        Py_DECREF(value);
        PyGILState_Release(state);
        return py_svn_error();
    }

    Py_DECREF(value);

    PyGILState_Release(state);

    return NULL;
}
#else
static svn_error_t *list_receiver(void *dict, const char *path,
                                  const svn_dirent_t *dirent,
                                  const svn_lock_t *lock, const char *abs_path,
                                  apr_pool_t *pool)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *value;

    value = py_dirent(dirent, SVN_DIRENT_ALL);
    if (value == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    if (PyDict_SetItemString(dict, path, value) != 0) {
        Py_DECREF(value);
        PyGILState_Release(state);
        return py_svn_error();
    }

    Py_DECREF(value);

    PyGILState_Release(state);

    return NULL;
}
#endif

#if ONLY_SINCE_SVN(1, 7)
static PyObject *py_info(const svn_client_info2_t *info)
#else
static PyObject *py_info(const svn_info_t *info)
#endif
{
    InfoObject *ret;

    ret = PyObject_New(InfoObject, &Info_Type);
    if (ret == NULL)
        return NULL;

    ret->wc_info = PyObject_New(WCInfoObject, &WCInfo_Type);
    if (ret->wc_info == NULL)
        return NULL;

    ret->pool = ret->wc_info->pool = Pool(NULL);
    if (ret->pool == NULL)
        return NULL;
#if ONLY_SINCE_SVN(1, 7)
    ret->info = *svn_client_info2_dup(info, ret->pool);
    if (info->wc_info != NULL)
        ret->wc_info->info = *svn_wc_info_dup(info->wc_info, ret->pool);
    else
        ret->wc_info = NULL;
#else
    ret->info = *svn_info_dup(info, ret->pool);
    if (info->has_wc_info) {
        ret->wc_info->info = *svn_info_dup(info, ret->pool);
    }
#endif

    return (PyObject *)ret;
}

static svn_error_t *info_receiver(void *dict, const char *path,
#if ONLY_BEFORE_SVN(1, 7)
                                  const svn_info_t *info,
#else
                                  const svn_client_info2_t *info,
#endif
                                  apr_pool_t *pool)
{
    PyGILState_STATE state = PyGILState_Ensure();
    PyObject *value;

    value = py_info(info);
    if (value == NULL) {
        PyGILState_Release(state);
        return py_svn_error();
    }

    if (PyDict_SetItemString(dict, path, value) != 0) {
        Py_DECREF(value);
        PyGILState_Release(state);
        return py_svn_error();
    }

    Py_DECREF(value);

    PyGILState_Release(state);

    return NULL;
}

static svn_error_t *py_log_msg_func2(const char **log_msg, const char **tmp_file, const apr_array_header_t *commit_items, void *baton, apr_pool_t *pool)
{
    PyObject *py_commit_items, *ret, *py_log_msg, *py_tmp_file;
    PyGILState_STATE state;

    if (baton == Py_None)
        return NULL;

    state = PyGILState_Ensure();
    py_commit_items = wrap_py_commit_items(commit_items);
    CB_CHECK_PYRETVAL(py_commit_items);

    ret = PyObject_CallFunction(baton, "O", py_commit_items);
    Py_DECREF(py_commit_items);
    CB_CHECK_PYRETVAL(ret);
    if (PyTuple_Check(ret)) {
        py_log_msg = PyTuple_GetItem(ret, 0);
        py_tmp_file = PyTuple_GetItem(ret, 1);
    } else {
        py_tmp_file = Py_None;
        py_log_msg = ret;
    }
    if (py_log_msg != Py_None) {
        *log_msg = py_object_to_svn_string(py_log_msg, pool);
    }
    if (py_tmp_file != Py_None) {
        *tmp_file = py_object_to_svn_string(py_tmp_file, pool);
    }
    Py_DECREF(ret);
    PyGILState_Release(state);
    return NULL;
}

static PyObject *py_commit_info_tuple(svn_commit_info_t *ci)
{
    if (ci == NULL)
        Py_RETURN_NONE;
    if (ci->revision == SVN_INVALID_REVNUM)
        Py_RETURN_NONE;
    return Py_BuildValue("(lzz)", ci->revision, ci->date, ci->author);
}

typedef struct {
    PyObject_VAR_HEAD
    svn_client_ctx_t *client;
    apr_pool_t *pool;
    PyObject *callbacks;
    PyObject *py_auth;
    PyObject *py_config;
} ClientObject;

static PyObject *client_new(PyTypeObject *type, PyObject *args, PyObject *kwargs)
{
    ClientObject *ret;
    PyObject *config = Py_None, *auth = Py_None, *log_msg_func = Py_None;
    char *kwnames[] = { "config", "auth", "log_msg_func", NULL };
    svn_error_t *err;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|OOO", kwnames,
        &config, &auth, &log_msg_func))
        return NULL;

    ret = PyObject_New(ClientObject, &Client_Type);
    if (ret == NULL)
        return NULL;

    ret->pool = Pool(NULL);
    if (ret->pool == NULL) {
        Py_DECREF(ret);
        return NULL;
    }

#if ONLY_SINCE_SVN(1, 8)
    err = svn_client_create_context2(&ret->client, NULL, ret->pool);
#else
    err = svn_client_create_context(&ret->client, ret->pool);
#endif
    if (err != NULL) {
        handle_svn_error(err);
        svn_error_clear(err);
        apr_pool_destroy(ret->pool);
        PyObject_Del(ret);
        return NULL;
    }

    ret->py_auth = NULL;
    ret->py_config = NULL;
    ret->client->notify_func2 = NULL;
    ret->client->notify_baton2 = NULL;
    ret->client->cancel_func = py_cancel_check;
    ret->client->cancel_baton = NULL;
    if (log_msg_func != Py_None) {
        ret->client->log_msg_func2 = py_log_msg_func2;
    } else {
        ret->client->log_msg_func2 = NULL;
    }
    Py_INCREF(log_msg_func);
    ret->client->log_msg_baton2 = (void *)log_msg_func;
    client_set_config((PyObject *)ret, config, NULL);
    client_set_auth((PyObject *)ret, auth, NULL);
    return (PyObject *)ret;
}

static void client_dealloc(PyObject *self)
{
    ClientObject *client = (ClientObject *)self;
    Py_XDECREF((PyObject *)client->client->notify_baton2);
    Py_XDECREF((PyObject *)client->client->log_msg_baton2);
    Py_XDECREF(client->py_auth);
    Py_XDECREF(client->py_config);
    if (client->pool != NULL)
        apr_pool_destroy(client->pool);
    PyObject_Del(self);
}

static PyObject *client_get_log_msg_func(PyObject *self, void *closure)
{
    ClientObject *client = (ClientObject *)self;
    if (client->client->log_msg_func2 == NULL)
        Py_RETURN_NONE;
    return client->client->log_msg_baton2;
}

static int client_set_log_msg_func(PyObject *self, PyObject *func, void *closure)
{
    ClientObject *client = (ClientObject *)self;

    if (client->client->log_msg_baton2 != NULL) {
        Py_DECREF((PyObject *)client->client->log_msg_baton2);
    }
    if (func == Py_None) {
        client->client->log_msg_func2 = NULL;
        client->client->log_msg_baton2 = Py_None;
    } else {
        client->client->log_msg_func2 = py_log_msg_func2;
        client->client->log_msg_baton2 = (void *)func;
    }
    Py_INCREF(func);
    return 0;
}

static PyObject *client_get_notify_func(PyObject *self, void *closure)
{
    ClientObject *client = (ClientObject *)self;
    if (client->client->notify_func2 == NULL)
        Py_RETURN_NONE;
    Py_INCREF((PyObject *)client->client->notify_baton2);
    return client->client->notify_baton2;
}

static int client_set_notify_func(PyObject *self, PyObject *func, void *closure)
{
    ClientObject *client = (ClientObject *)self;

    if (client->client->notify_baton2 != NULL) {
        Py_DECREF((PyObject *)client->client->notify_baton2);
    }
    if (func == Py_None) {
        client->client->notify_func2 = NULL;
        client->client->notify_baton2 = Py_None;
    } else {
        client->client->notify_func2 = py_wc_notify_func;
        client->client->notify_baton2 = (void *)func;
    }
    Py_INCREF(func);
    return 0;
}

static int client_set_auth(PyObject *self, PyObject *auth, void *closure)
{
    ClientObject *client = (ClientObject *)self;
    apr_array_header_t *auth_providers;

    Py_XDECREF(client->py_auth);


    if (auth == Py_None) {
        auth_providers = apr_array_make(client->pool, 0, sizeof(svn_auth_provider_object_t *));
        if (auth_providers == NULL) {
            PyErr_NoMemory();
            return 1;
        }
        Py_BEGIN_ALLOW_THREADS
        svn_auth_open(&client->client->auth_baton, auth_providers, client->pool);
        Py_END_ALLOW_THREADS
    } else {
        client->client->auth_baton = ((AuthObject *)auth)->auth_baton;
    }

    client->py_auth = auth;
    Py_INCREF(auth);

    return 0;
}

static int client_set_config(PyObject *self, PyObject *config, void *closure)
{
    ClientObject *client = (ClientObject *)self;

    Py_XDECREF(client->py_config);

    client->client->config = config_hash_from_object(config, client->pool);

    if (client->client->config == NULL) {
        client->py_config = NULL;
        return -1;
    }

    client->py_config = config;
    Py_INCREF(config);

    return 0;
}

static PyObject *client_add(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *path;
    ClientObject *client = (ClientObject *)self;
    bool recursive=true, force=false, no_ignore=false;
    bool add_parents = false;
    bool no_autoprops = false;
    apr_pool_t *temp_pool;
    char *kwnames[] = { "path", "recursive", "force", "no_ignore",
                        "add_parents", "no_autoprops", NULL };

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|bbbbb", kwnames,
                          &path, &recursive, &force, &no_ignore, &add_parents, &no_autoprops))
        return NULL;

#if ONLY_BEFORE_SVN(1, 4)
    if (add_parents == false) {
        PyErr_SetString(PyExc_NotImplementedError,
            "Subversion < 1.4 does not support add_parents=false");
        return NULL;
    }
#endif

#if ONLY_BEFORE_SVN(1, 8)
    if (no_autoprops) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "Subversion < 1.8 does not support no_autoprops");
        return NULL;
    }
#endif

    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

#if ONLY_SINCE_SVN(1, 8)
    RUN_SVN_WITH_POOL(temp_pool,
        svn_client_add5(path, recursive?svn_depth_infinity:svn_depth_empty,
                        force, no_ignore, no_autoprops, add_parents,
                        client->client, temp_pool)
        );
#elif ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool,
        svn_client_add4(path, recursive?svn_depth_infinity:svn_depth_empty,
                        force, no_ignore, add_parents,
                        client->client, temp_pool)
        );
#else
    RUN_SVN_WITH_POOL(temp_pool,
        svn_client_add3(path, recursive, force, no_ignore, client->client,
                        temp_pool)
        );
#endif
    apr_pool_destroy(temp_pool);
    Py_RETURN_NONE;
}

static PyObject *client_checkout(PyObject *self, PyObject *args, PyObject *kwargs)
{
    ClientObject *client = (ClientObject *)self;
    char *kwnames[] = { "url", "path", "rev", "peg_rev", "recurse", "ignore_externals", "allow_unver_obstructions", NULL };
    svn_revnum_t result_rev;
    svn_opt_revision_t c_peg_rev, c_rev;
    const char *path;
    const char *url;
    PyObject *py_url = NULL, *py_path;
    apr_pool_t *temp_pool;
    PyObject *peg_rev=Py_None, *rev=Py_None;
    bool recurse=true, ignore_externals=false, allow_unver_obstructions=false;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|OObbb", kwnames,
                                     &py_url, &py_path, &rev, &peg_rev,
                                     &recurse, &ignore_externals,
                                     &allow_unver_obstructions)) {
        return NULL;
    }

    if (!to_opt_revision(peg_rev, &c_peg_rev)) {
        return NULL;
    }

    if (!to_opt_revision(rev, &c_rev)) {
        return NULL;
    }

    temp_pool = Pool(NULL);
    if (temp_pool == NULL) {
        return NULL;
    }

    url = py_object_to_svn_uri(py_url, temp_pool);
    if (url == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    path = py_object_to_svn_dirent(py_path, temp_pool);
    if (path == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

#if ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool, svn_client_checkout3(&result_rev, url,
        path,
        &c_peg_rev, &c_rev, recurse?svn_depth_infinity:svn_depth_files,
        ignore_externals, allow_unver_obstructions, client->client, temp_pool));
#else
    if (allow_unver_obstructions) {
        PyErr_SetString(PyExc_NotImplementedError,
            "allow_unver_obstructions not supported when built against svn<1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    RUN_SVN_WITH_POOL(temp_pool, svn_client_checkout2(&result_rev, url,
        path,
        &c_peg_rev, &c_rev, recurse,
        ignore_externals, client->client, temp_pool));
#endif
    apr_pool_destroy(temp_pool);
    return PyLong_FromLong(result_rev);
}

static PyObject *client_commit(PyObject *self, PyObject *args, PyObject *kwargs)
{
    PyObject *targets;
    ClientObject *client = (ClientObject *)self;
    bool recurse=true, keep_locks=true;
    apr_pool_t *temp_pool;
    svn_commit_info_t *commit_info = NULL;
    PyObject *ret;
    apr_array_header_t *apr_targets;
    PyObject *revprops = Py_None;
    char *kwnames[] = { "targets", "recurse", "keep_locks", "revprops", NULL };
#if ONLY_SINCE_SVN(1, 5)
    apr_hash_t *hash_revprops;
#endif
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|bbO", kwnames, &targets, &recurse, &keep_locks, &revprops))
        return NULL;
    temp_pool = Pool(NULL);
    if (temp_pool == NULL) {
        return NULL;
    }
    if (!client_path_list_to_apr_array(temp_pool, targets, &apr_targets)) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    if (revprops != Py_None && !PyDict_Check(revprops)) {
        apr_pool_destroy(temp_pool);
        PyErr_SetString(PyExc_TypeError, "Expected dictionary with revision properties");
        return NULL;
    }


#if ONLY_SINCE_SVN(1, 5)
    if (revprops != Py_None) {
        hash_revprops = prop_dict_to_hash(temp_pool, revprops);
        if (hash_revprops == NULL) {
            apr_pool_destroy(temp_pool);
            return NULL;
        }
    } else {
        hash_revprops = NULL;
    }

    /* FIXME: Support keep_changelist and changelists */
    RUN_SVN_WITH_POOL(temp_pool, svn_client_commit4(&commit_info,
                                                    apr_targets, recurse?svn_depth_infinity:svn_depth_files,
                                                    keep_locks, false, NULL, hash_revprops,
                                                    client->client, temp_pool));
#else
    if (revprops != Py_None && PyDict_Size(revprops) > 0) {
        PyErr_SetString(PyExc_NotImplementedError,
                "Setting revision properties only supported on svn >= 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    RUN_SVN_WITH_POOL(temp_pool, svn_client_commit3(&commit_info,
                apr_targets,
               recurse, keep_locks, client->client, temp_pool));
#endif
    ret = py_commit_info_tuple(commit_info);
    apr_pool_destroy(temp_pool);

    return ret;
}

static PyObject *client_export(PyObject *self, PyObject *args, PyObject *kwargs)
{
    ClientObject *client = (ClientObject *)self;
    char *kwnames[] = { "from", "to", "rev", "peg_rev", "recurse", "ignore_externals", "overwrite", "native_eol", "ignore_keywords", NULL };
    svn_revnum_t result_rev;
    svn_opt_revision_t c_peg_rev, c_rev;
	PyObject *py_from, *py_to;
    const char *from, *to;
    apr_pool_t *temp_pool;
	char *native_eol = NULL;
    PyObject *peg_rev=Py_None, *rev=Py_None;
    bool recurse=true, ignore_externals=false, overwrite=false, ignore_keywords=false;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|OObbbbb", kwnames, &py_from, &py_to, &rev, &peg_rev, &recurse, &ignore_externals, &overwrite, &native_eol, &ignore_keywords))
        return NULL;

    if (!to_opt_revision(peg_rev, &c_peg_rev))
        return NULL;
    if (!to_opt_revision(rev, &c_rev))
        return NULL;

    temp_pool = Pool(NULL);
    if (temp_pool == NULL) {
        return NULL;
    }

	from = py_object_to_svn_string(py_from, temp_pool);
	if (from == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	to = py_object_to_svn_dirent(py_to, temp_pool);
	if (to == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

#if ONLY_SINCE_SVN(1, 7)
	RUN_SVN_WITH_POOL(temp_pool, svn_client_export5(&result_rev, from, to,
        &c_peg_rev, &c_rev, overwrite, ignore_externals, ignore_keywords,
        recurse?svn_depth_infinity:svn_depth_files,
        native_eol, client->client, temp_pool));
#elif ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool, svn_client_export4(&result_rev, from, to,
        &c_peg_rev, &c_rev, overwrite, ignore_externals,
		recurse?svn_depth_infinity:svn_depth_files,
        native_eol, client->client, temp_pool));
#else
    RUN_SVN_WITH_POOL(temp_pool, svn_client_export3(&result_rev, from, to,
        &c_peg_rev, &c_rev, overwrite, ignore_externals, recurse,
        native_eol, client->client, temp_pool));
#endif
    apr_pool_destroy(temp_pool);
    return PyLong_FromLong(result_rev);
}

static PyObject *client_cat(PyObject *self, PyObject *args, PyObject *kwargs)
{
	ClientObject *client = (ClientObject *)self;
	char *kwnames[] = { "path", "output_stream", "revision", "peg_revision", NULL };
	char *path;
	PyObject *peg_rev=Py_None, *rev=Py_None;
	svn_opt_revision_t c_peg_rev, c_rev;
	apr_pool_t *temp_pool;
	svn_stream_t *stream;
	bool expand_keywords = true;
	PyObject *py_stream, *py_path, *ret;
	apr_hash_t *props = NULL;

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|OOb", kwnames, &py_path, &py_stream, &rev, &peg_rev, &expand_keywords))
		return NULL;

	if (!to_opt_revision(rev, &c_rev))
		return NULL;
	if (!to_opt_revision(peg_rev, &c_peg_rev))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL) {
		return NULL;
	}

	path = py_object_to_svn_string(py_path, temp_pool);
	if (path == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	stream = new_py_stream(temp_pool, py_stream);
	if (stream == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

#if ONLY_SINCE_SVN(1, 9)
	RUN_SVN_WITH_POOL(temp_pool, svn_client_cat3(
		&props, stream, path, &c_peg_rev, &c_rev, expand_keywords,
		client->client, temp_pool, temp_pool));

	ret = prop_hash_to_dict(props);
	if (ret == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}
#else
	if (!expand_keywords) {
		PyErr_SetString(PyExc_NotImplementedError,
						"expand_keywords=false only supported with svn >= 1.9");
		apr_pool_destroy(temp_pool);
		return NULL;
	}
	RUN_SVN_WITH_POOL(temp_pool, svn_client_cat2(stream, path,
		&c_peg_rev, &c_rev, client->client, temp_pool));

	ret = Py_None;
	Py_INCREF(ret);
#endif

	apr_pool_destroy(temp_pool);
	return ret;
}

static PyObject *client_delete(PyObject *self, PyObject *args)
{
	PyObject *paths;
	bool force=false, keep_local=false;
	apr_pool_t *temp_pool;
	svn_commit_info_t *commit_info = NULL;
	PyObject *ret, *py_revprops;
	apr_array_header_t *apr_paths;
	ClientObject *client = (ClientObject *)self;
	apr_hash_t *hash_revprops;

	if (!PyArg_ParseTuple(args, "O|bbO", &paths, &force, &keep_local, &py_revprops))
		return NULL;

	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	if (!client_path_list_to_apr_array(temp_pool, paths, &apr_paths)) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	if (py_revprops != Py_None) {
		hash_revprops = prop_dict_to_hash(temp_pool, py_revprops);
		if (hash_revprops == NULL) {
			apr_pool_destroy(temp_pool);
			return NULL;
		}
	} else {
		hash_revprops = NULL;
	}

#if ONLY_SINCE_SVN(1, 5)
	RUN_SVN_WITH_POOL(temp_pool, svn_client_delete3(
		&commit_info, apr_paths, force, keep_local, hash_revprops, client->client, temp_pool));
#else
	if (hash_revprops != NULL) {
		PyErr_SetString(PyExc_NotImplementedError,
                        "revprops not supported against svn 1.4");
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	if (keep_local) {
		PyErr_SetString(PyExc_NotImplementedError,
                        "keep_local not supported against svn 1.4");
		apr_pool_destroy(temp_pool);
		return NULL;
	}
	RUN_SVN_WITH_POOL(temp_pool, svn_client_delete2(
		&commit_info, apr_paths, force, client->client, temp_pool));
#endif

	ret = py_commit_info_tuple(commit_info);

	apr_pool_destroy(temp_pool);

	return ret;
}

static PyObject *client_mkdir(PyObject *self, PyObject *args)
{
    PyObject *paths, *revprops = NULL;
    bool make_parents = false;
    apr_pool_t *temp_pool;
    svn_commit_info_t *commit_info = NULL;
    PyObject *ret;
    apr_array_header_t *apr_paths;
    apr_hash_t *hash_revprops;
    ClientObject *client = (ClientObject *)self;

    if (!PyArg_ParseTuple(args, "O|bO", &paths, &make_parents, &revprops))
        return NULL;

    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;
    if (!client_path_list_to_apr_array(temp_pool, paths, &apr_paths)) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    if (revprops != NULL && !PyDict_Check(revprops)) {
        apr_pool_destroy(temp_pool);
        PyErr_SetString(PyExc_TypeError, "Expected dictionary with revision properties");
        return NULL;
    }

#if ONLY_SINCE_SVN(1, 5)
    if (revprops != NULL && revprops != Py_None) {
        hash_revprops = prop_dict_to_hash(temp_pool, revprops);
        if (hash_revprops == NULL) {
            apr_pool_destroy(temp_pool);
            return NULL;
        }
    } else {
        hash_revprops = NULL;
    }

    RUN_SVN_WITH_POOL(temp_pool, svn_client_mkdir3(&commit_info,
                                                    apr_paths,
                make_parents?TRUE:FALSE, hash_revprops, client->client, temp_pool));
#else
    if (make_parents) {
        PyErr_SetString(PyExc_ValueError,
                        "make_parents not supported against svn 1.4");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    if (revprops != Py_None) {
        PyErr_SetString(PyExc_ValueError,
                        "revprops not supported against svn 1.4");
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    RUN_SVN_WITH_POOL(temp_pool, svn_client_mkdir2(&commit_info,
                                                    apr_paths,
                client->client, temp_pool));
#endif

    ret = py_commit_info_tuple(commit_info);

    apr_pool_destroy(temp_pool);

    return ret;
}



static PyObject *client_copy(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *src_path, *dst_path;
    PyObject *src_rev = Py_None;
    svn_commit_info_t *commit_info = NULL;
    apr_pool_t *temp_pool;
    svn_opt_revision_t c_src_rev;
    bool copy_as_child = true, make_parents = false;
    PyObject *ret;
    apr_hash_t *revprops;
    bool ignore_externals = false;
    ClientObject *client = (ClientObject *)self;
    char *kwnames[] = { "src_path", "dst_path", "src_rev", "copy_as_child",
        "make_parents", "ignore_externals", "revprpos", NULL };

#if ONLY_SINCE_SVN(1, 4)
    PyObject *py_revprops = Py_None;
#endif
#if ONLY_SINCE_SVN(1, 5)
    apr_array_header_t *src_paths;
    svn_client_copy_source_t src;
#endif

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ss|ObbbO", kwnames,
             &src_path, &dst_path, &src_rev, &copy_as_child, &make_parents,
             &ignore_externals, &py_revprops))
        return NULL;
    if (!to_opt_revision(src_rev, &c_src_rev))
        return NULL;
    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

    if (py_revprops != Py_None) {
        revprops = prop_dict_to_hash(temp_pool, py_revprops);
        if (revprops == NULL) {
            apr_pool_destroy(temp_pool);
            return NULL;
        }
    } else {
        revprops = NULL;
    }

#if ONLY_BEFORE_SVN(1, 4)
    if (copy_as_child) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "copy_as_child not supported in svn < 1.4");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    if (make_parents) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "make_parents not supported in svn < 1.4");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    if (revprops) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "revprops not supported in svn < 1.4");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
#endif
#if ONLY_BEFORE_SVN(1, 5)
    if (ignore_externals) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "ignore_externals not supported in svn < 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
#endif
#if ONLY_SINCE_SVN(1, 5)
    src.path = src_path;
    src.revision = src.peg_revision = &c_src_rev;
    src_paths = apr_array_make(temp_pool, 1, sizeof(svn_client_copy_source_t *));
    if (src_paths == NULL) {
        PyErr_NoMemory();
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    APR_ARRAY_PUSH(src_paths, svn_client_copy_source_t *) = &src;
#endif
#if ONLY_SINCE_SVN(1, 6)
    RUN_SVN_WITH_POOL(temp_pool, svn_client_copy5(&commit_info, src_paths,
                dst_path, copy_as_child, make_parents,
                ignore_externals, revprops, client->client, temp_pool));
#elif ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool, svn_client_copy4(&commit_info, src_paths,
                dst_path, copy_as_child, make_parents,
                revprops, client->client, temp_pool));
#else
    RUN_SVN_WITH_POOL(temp_pool, svn_client_copy2(&commit_info, src_path,
                &c_src_rev, dst_path, client->client, temp_pool));
#endif
    ret = py_commit_info_tuple(commit_info);
    apr_pool_destroy(temp_pool);
    return ret;
}

static PyObject *client_propset(PyObject *self, PyObject *args)
{
    char *propname;
    svn_string_t c_propval;
    int vallen;
    int recurse = true;
    int skip_checks = false;
    ClientObject *client = (ClientObject *)self;
    apr_pool_t *temp_pool;
    char *target;
#if ONLY_SINCE_SVN(1, 5)
    svn_commit_info_t *commit_info = NULL;
#endif
    PyObject *ret, *py_revprops = Py_None;
    svn_revnum_t base_revision_for_url = SVN_INVALID_REVNUM;
    apr_hash_t *revprops;

    if (!PyArg_ParseTuple(args, "sz#s|bblO", &propname, &c_propval.data,
                          &vallen, &target, &recurse, &skip_checks,
                          &base_revision_for_url, &py_revprops))
        return NULL;

    c_propval.len = vallen;

    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

    if (py_revprops != Py_None) {
        revprops = prop_dict_to_hash(temp_pool, py_revprops);
        if (revprops == NULL) {
            apr_pool_destroy(temp_pool);
            return NULL;
        }
    } else {
        revprops = NULL;
    }

#if ONLY_SINCE_SVN(1, 5)
    /* FIXME: Support changelists */
    /* FIXME: Support depth */
    RUN_SVN_WITH_POOL(temp_pool, svn_client_propset3(&commit_info, propname,
                c_propval.len==0?NULL:&c_propval, target, recurse?svn_depth_infinity:svn_depth_files,
                skip_checks, base_revision_for_url,
                NULL, revprops, client->client, temp_pool));
    ret = py_commit_info_tuple(commit_info);
#else
    if (revprops) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "revprops not supported with svn < 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    RUN_SVN_WITH_POOL(temp_pool, svn_client_propset2(propname, c_propval.len==0?NULL:&c_propval,
                target, recurse, skip_checks, client->client, temp_pool));
    ret = Py_None;
    Py_INCREF(ret);
#endif
    apr_pool_destroy(temp_pool);
    return ret;
}

static PyObject *client_propget(PyObject *self, PyObject *args)
{
    svn_opt_revision_t c_peg_rev;
    svn_opt_revision_t c_rev;
    apr_hash_t *hash_props;
    bool recurse = false;
    char *propname;
    apr_pool_t *temp_pool;
    char *target;
    PyObject *peg_revision = Py_None;
    PyObject *revision;
    ClientObject *client = (ClientObject *)self;
    PyObject *ret;

    if (!PyArg_ParseTuple(args, "ssO|Ob", &propname, &target, &peg_revision,
                          &revision, &recurse))
        return NULL;
    if (!to_opt_revision(peg_revision, &c_peg_rev))
        return NULL;
    if (!to_opt_revision(revision, &c_rev))
        return NULL;
    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;
#if ONLY_SINCE_SVN(1, 8)
    /* FIXME: Support changelists */
    /* FIXME: Support actual_revnum */
    /* FIXME: Support depth properly */
	/* FIXME: Support inherited_props */
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_propget5(&hash_props, NULL,
										  propname, target,
                &c_peg_rev, &c_rev, NULL, recurse?svn_depth_infinity:svn_depth_files,
                NULL, client->client, temp_pool, temp_pool));
#elif ONLY_SINCE_SVN(1, 5)
    /* FIXME: Support changelists */
    /* FIXME: Support actual_revnum */
    /* FIXME: Support depth properly */
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_propget3(&hash_props, propname, target,
                &c_peg_rev, &c_rev, NULL, recurse?svn_depth_infinity:svn_depth_files,
                NULL, client->client, temp_pool));
#else
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_propget2(&hash_props, propname, target,
                &c_peg_rev, &c_rev, recurse, client->client, temp_pool));
#endif
    ret = prop_hash_to_dict(hash_props);
    apr_pool_destroy(temp_pool);
    return ret;
}

static PyObject *client_proplist(PyObject *self, PyObject *args,
                                 PyObject *kwargs)
{
    char *kwnames[] = { "target", "peg_revision", "depth", "revision", NULL };
    svn_opt_revision_t c_peg_rev;
    svn_opt_revision_t c_rev;
    int depth;
    apr_pool_t *temp_pool;
    char *target;
    PyObject *peg_revision = Py_None, *revision = Py_None;
    ClientObject *client = (ClientObject *)self;
    PyObject *prop_list;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sOi|O", kwnames,
                                     &target, &peg_revision, &depth, &revision))
        return NULL;
    if (!to_opt_revision(peg_revision, &c_peg_rev))
        return NULL;
    if (!to_opt_revision(revision, &c_rev))
        return NULL;
    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

    prop_list = PyList_New(0);
    if (prop_list == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

#if ONLY_SINCE_SVN(1, 8)
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_proplist4(target, &c_peg_rev, &c_rev,
                                           depth, NULL,
                                           false, /* TODO(jelmer): Support get_target_inherited_props */
                                           proplist_receiver2, prop_list,
                                           client->client, temp_pool));

    apr_pool_destroy(temp_pool);
#elif ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_proplist3(target, &c_peg_rev, &c_rev,
                                           depth, NULL,
                                           proplist_receiver, prop_list,
                                           client->client, temp_pool));

    apr_pool_destroy(temp_pool);
#else
    {
        apr_array_header_t *props;
        int i;


    if (depth != svn_depth_infinity && depth != svn_depth_empty) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "depth can only be infinity or empty when built against svn < 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }


	RUN_SVN_WITH_POOL(temp_pool,
					  svn_client_proplist2(&props, target, &c_peg_rev, &c_rev,
										   (depth == svn_depth_infinity),
										   client->client, temp_pool));

	for (i = 0; i < props->nelts; i++) {
		svn_client_proplist_item_t *item;
		PyObject *prop_dict, *value;

		item = APR_ARRAY_IDX(props, i, svn_client_proplist_item_t *);

		prop_dict = prop_hash_to_dict(item->prop_hash);
		if (prop_dict == NULL) {
			apr_pool_destroy(temp_pool);
			Py_DECREF(prop_list);
			return NULL;
		}

		value = Py_BuildValue("(sO)", item->node_name, prop_dict);
		if (value == NULL) {
			apr_pool_destroy(temp_pool);
			Py_DECREF(prop_list);
			Py_DECREF(prop_dict);
			return NULL;
		}
		if (PyList_Append(prop_list, value) != 0) {
			apr_pool_destroy(temp_pool);
			Py_DECREF(prop_list);
			Py_DECREF(prop_dict);
			Py_DECREF(value);
			return NULL;
		}
		Py_DECREF(value);
    }

    apr_pool_destroy(temp_pool);

    }
#endif

    return prop_list;
}

static PyObject* client_revproplist(PyObject* self, PyObject* args,
                                    PyObject* kwargs) {
    char* kwnames[] = {"url", "revision", NULL};
    svn_opt_revision_t c_rev;
    svn_revnum_t revnum;
    apr_pool_t* temp_pool;
    char* url;
    PyObject* revision = Py_None;
    ClientObject* client = (ClientObject*)self;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sO", kwnames, &url, &revision))
        return NULL;
    if (!to_opt_revision(revision, &c_rev))
        return NULL;
    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

#if ONLY_SINCE_SVN(1, 5)
    apr_hash_t* props;
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_revprop_list(&props, url, &c_rev,
                                              &revnum, client->client, temp_pool));
    PyObject* prop_dict = prop_hash_to_dict(props);
    apr_pool_destroy(temp_pool);
#else
    error "Not implemented for SVN < 1.5"
#endif

    return prop_dict;
}

static PyObject *client_resolve(PyObject *self, PyObject *args)
{
#if ONLY_SINCE_SVN(1, 5)
    svn_depth_t depth;
    svn_wc_conflict_choice_t choice;
    ClientObject *client = (ClientObject *)self;
    apr_pool_t *temp_pool;
    char *path;

    if (!PyArg_ParseTuple(args, "sii", &path, &depth, &choice))
        return NULL;

    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;
    RUN_SVN_WITH_POOL(temp_pool, svn_client_resolve(path, depth, choice,
            client->client, temp_pool));

    apr_pool_destroy(temp_pool);

    Py_RETURN_NONE;
#else
    PyErr_SetString(PyExc_NotImplementedError,
        "svn_client_resolve not available with Subversion < 1.5");
    return NULL;
#endif
}

static PyObject *client_update(PyObject *self, PyObject *args, PyObject *kwargs)
{
    bool recurse = true;
    bool ignore_externals = false;
    apr_pool_t *temp_pool;
    PyObject *rev = Py_None, *paths;
    apr_array_header_t *result_revs, *apr_paths;
    svn_opt_revision_t c_rev;
    svn_revnum_t ret_rev;
    PyObject *ret;
    int i = 0;
    ClientObject *client = (ClientObject *)self;
    bool allow_unver_obstructions = false,
                  depth_is_sticky = false,
              adds_as_modification = true,
	                 make_parents = false;
	char *kwnames[] =
		{ "path", "revision", "recurse", "ignore_externals", "depth_is_sticky",
			"allow_unver_obstructions", "adds_as_modification", "make_parents", NULL };

	if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|Obbbbbb", kwnames,
			&paths, &rev, &recurse, &ignore_externals,
			&depth_is_sticky, &allow_unver_obstructions, &adds_as_modification,
			&make_parents))
		return NULL;

	if (!to_opt_revision(rev, &c_rev))
		return NULL;
	temp_pool = Pool(NULL);
	if (temp_pool == NULL)
		return NULL;
	if (!client_path_list_to_apr_array(temp_pool, paths, &apr_paths)) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

#if ONLY_BEFORE_SVN(1, 7)
	if (!adds_as_modification) {
		PyErr_SetString(PyExc_NotImplementedError,
						"!adds_as_modification not supported before svn 1.7");
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	if (make_parents) {
		PyErr_SetString(PyExc_NotImplementedError,
						"make_parents not supported before svn 1.7");
		apr_pool_destroy(temp_pool);
		return NULL;
	}
#endif

#if ONLY_SINCE_SVN(1, 7)
	RUN_SVN_WITH_POOL(temp_pool, svn_client_update4(&result_revs,
		apr_paths, &c_rev, recurse?svn_depth_infinity:svn_depth_files,
		depth_is_sticky?TRUE:FALSE, ignore_externals, allow_unver_obstructions?TRUE:FALSE,
		adds_as_modification?TRUE:FALSE, make_parents?TRUE:FALSE,
		client->client, temp_pool));
#elif ONLY_SINCE_SVN(1, 5)
	RUN_SVN_WITH_POOL(temp_pool, svn_client_update3(&result_revs,
		apr_paths, &c_rev, recurse?svn_depth_infinity:svn_depth_files,
		depth_is_sticky?TRUE:FALSE, ignore_externals, allow_unver_obstructions?TRUE:FALSE,
		client->client, temp_pool));
#else
	RUN_SVN_WITH_POOL(temp_pool, svn_client_update2(&result_revs,
													apr_paths, &c_rev,
													recurse, ignore_externals, client->client, temp_pool));
#endif
	ret = PyList_New(result_revs->nelts);
	if (ret == NULL) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}
	for (i = 0; i < result_revs->nelts; i++) {
		ret_rev = APR_ARRAY_IDX(result_revs, i, svn_revnum_t);
		if (PyList_SetItem(ret, i, PyLong_FromLong(ret_rev)) != 0) {
			Py_DECREF(ret);
			return NULL;
		}
	}
	apr_pool_destroy(temp_pool);
	return ret;
}

static PyObject *client_list(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *kwnames[] =
        { "path", "peg_revision", "depth", "dirents", "revision", NULL };
    svn_opt_revision_t c_peg_rev;
    svn_opt_revision_t c_rev;
    int depth;
    int dirents = SVN_DIRENT_ALL;
    apr_pool_t *temp_pool;
    char *path;
    bool include_externals = false;
    PyObject *peg_revision = Py_None, *revision = Py_None;
    ClientObject *client = (ClientObject *)self;
    PyObject *entry_dict;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "sOi|iOb", kwnames,
                                     &path, &peg_revision, &depth, &dirents,
                                     &revision, &include_externals))
        return NULL;

    if (!to_opt_revision(peg_revision, &c_peg_rev))
        return NULL;
    if (!to_opt_revision(revision, &c_rev))
        return NULL;
    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;
    entry_dict = PyDict_New();
    if (entry_dict == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

#if ONLY_BEFORE_SVN(1, 8)
    if (include_externals) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "include_externals requires svn >= 1.8");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
#endif

#if ONLY_SINCE_SVN(1, 8)
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_list3(path, &c_peg_rev, &c_rev,
                                       depth, dirents, false,
                                       include_externals,
                                       list_receiver2, entry_dict,
                                       client->client, temp_pool));
#elif ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_list2(path, &c_peg_rev, &c_rev,
                                       depth, dirents, false,
                                       list_receiver, entry_dict,
                                       client->client, temp_pool));
#else
    if (depth != svn_depth_infinity && depth != svn_depth_empty) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "depth can only be infinity or empty when built against svn < 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_list(path, &c_peg_rev, &c_rev,
                                       (depth == svn_depth_infinity)?TRUE:FALSE,
                                       dirents, false,
                                       list_receiver, entry_dict,
                                       client->client, temp_pool));
#endif
    apr_pool_destroy(temp_pool);

    return entry_dict;
}

static PyObject *client_diff(PyObject *self, PyObject *args, PyObject *kwargs)
{
#if ONLY_SINCE_SVN(1, 5)
    char *kwnames[] = {
        "rev1", "rev2", "path1", "path2",
        "relative_to_dir", "diffopts", "encoding",
        "ignore_ancestry", "no_diff_deleted", "ignore_content_type",
        NULL,
    };
    apr_pool_t *temp_pool;
    ClientObject *client = (ClientObject *)self;

    svn_opt_revision_t c_rev1, c_rev2;
    svn_depth_t depth = svn_depth_infinity;
    char *path1 = NULL, *path2 = NULL, *relative_to_dir = NULL;
    char *encoding = "utf-8";
    PyObject *rev1 = Py_None, *rev2 = Py_None;
    int ignore_ancestry = true, no_diff_deleted = true,
        ignore_content_type = false;
    PyObject *diffopts = Py_None;
    apr_array_header_t *c_diffopts;
    PyObject *outfile, *errfile;
    apr_file_t *c_outfile, *c_errfile;
    apr_off_t offset;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|zzzOsbbb:diff", kwnames,
                                     &rev1, &rev2, &path1, &path2,
                                     &relative_to_dir, &diffopts, &encoding,
                                     &ignore_ancestry, &no_diff_deleted,
                                     &ignore_content_type))
        return NULL;

    if (!to_opt_revision(rev1, &c_rev1) || !to_opt_revision(rev2, &c_rev2))
        return NULL;

    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

    if (diffopts == Py_None)
        diffopts = PyList_New(0);
    else
        Py_INCREF(diffopts);

    if (diffopts == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    if (!string_list_to_apr_array(temp_pool, diffopts, &c_diffopts)) {
        apr_pool_destroy(temp_pool);
        Py_DECREF(diffopts);
        return NULL;
    }
    Py_DECREF(diffopts);

    outfile = PyOS_tmpfile();
    if (outfile == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    errfile = PyOS_tmpfile();
    if (errfile == NULL) {
        apr_pool_destroy(temp_pool);
        Py_DECREF(outfile);
        return NULL;
    }

    c_outfile = apr_file_from_object(outfile, temp_pool);
    if (c_outfile == NULL) {
        apr_pool_destroy(temp_pool);
        Py_DECREF(outfile);
        Py_DECREF(errfile);
        return NULL;
    }

    c_errfile = apr_file_from_object(errfile, temp_pool);
    if (c_errfile == NULL) {
        apr_pool_destroy(temp_pool);
        Py_DECREF(outfile);
        Py_DECREF(errfile);
        return NULL;
    }

    RUN_SVN_WITH_POOL(temp_pool,
                      svn_client_diff4(c_diffopts,
                                       path1, &c_rev1, path2, &c_rev2,
                                       relative_to_dir, depth,
                                       ignore_ancestry, no_diff_deleted,
                                       ignore_content_type, encoding,
                                       c_outfile, c_errfile, NULL,
                                       client->client, temp_pool));

    offset = 0;
    apr_file_seek(c_outfile, APR_SET, &offset);
    offset = 0;
    apr_file_seek(c_errfile, APR_SET, &offset);

    apr_pool_destroy(temp_pool);

    return Py_BuildValue("(NN)", outfile, errfile);
#else
    PyErr_SetString(PyExc_NotImplementedError,
                    "svn_client_diff4 not available with Subversion < 1.5");
    return NULL;
#endif
}

static PyObject *client_log(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *kwnames[] = {
        "callback", "paths", "start_rev", "end_rev", "limit", "peg_revision",
        "discover_changed_paths", "strict_node_history",
        "include_merged_revisions", "revprops",
        NULL,
    };
    apr_pool_t *temp_pool;
    ClientObject *client = (ClientObject *)self;

    PyObject *callback, *paths, *start_rev = Py_None, *end_rev = Py_None,
             *peg_revision = Py_None, *revprops = NULL;
    int limit = 0;
    bool discover_changed_paths = false, strict_node_history = false,
                  include_merged_revisions = false;
    apr_array_header_t *apr_paths, *apr_revprops = NULL;
    svn_opt_revision_t c_peg_rev, c_start_rev, c_end_rev;
#if ONLY_SINCE_SVN(1, 6)
    svn_opt_revision_range_t revision_range;
    apr_array_header_t *revision_ranges;
#endif

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "OO|OOiObbbO", kwnames,
                                     &callback, &paths, &start_rev, &end_rev, &limit,
                                     &peg_revision, &discover_changed_paths,
                                     &strict_node_history, &include_merged_revisions,
                                     &revprops))
        return NULL;

    if (!to_opt_revision(start_rev, &c_start_rev))
        return NULL;
    if (!to_opt_revision(end_rev, &c_end_rev))
        return NULL;
    if (!to_opt_revision(peg_revision, &c_peg_rev))
        return NULL;

    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

#if ONLY_BEFORE_SVN(1, 5)
    if (include_merged_revisions) {
        PyErr_SetString(PyExc_NotImplementedError, 
                        "include_merged_revisions not supported in svn < 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    if (revprops) {
        PyErr_SetString(PyExc_NotImplementedError, 
                        "revprops not supported in svn < 1.5");
        apr_pool_destroy(temp_pool);
        return NULL;
    }
#endif

	if (!client_path_list_to_apr_array(temp_pool, paths, &apr_paths)) {
		apr_pool_destroy(temp_pool);
		return NULL;
	}

	if (revprops) {
		if (!string_list_to_apr_array(temp_pool, revprops, &apr_revprops)) {
			apr_pool_destroy(temp_pool);
			return NULL;
		}
	}

#if ONLY_SINCE_SVN(1, 6)
    revision_range.start = c_start_rev;
    revision_range.end = c_end_rev;

    revision_ranges = apr_array_make(temp_pool, 1, sizeof(svn_opt_revision_range_t *));
    if (revision_ranges == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }
    APR_ARRAY_PUSH(revision_ranges, svn_opt_revision_range_t *) = &revision_range;

    RUN_SVN_WITH_POOL(temp_pool, svn_client_log5(apr_paths, &c_peg_rev,
        revision_ranges, limit, discover_changed_paths?TRUE:FALSE,
        strict_node_history?TRUE:FALSE, include_merged_revisions?TRUE:FALSE, apr_revprops,
        py_svn_log_entry_receiver, (void*)callback,
        client->client, temp_pool));
#elif ONLY_SINCE_SVN(1, 5)
    RUN_SVN_WITH_POOL(temp_pool, svn_client_log4(apr_paths, &c_peg_rev,
        &c_start_rev, &c_end_rev, limit, discover_changed_paths?TRUE:FALSE,
        strict_node_history?TRUE:FALSE, include_merged_revisions?TRUE:FALSE, apr_revprops,
        py_svn_log_entry_receiver, (void*)callback,
        client->client, temp_pool));
#elif ONLY_SINCE_SVN(1, 4)
    RUN_SVN_WITH_POOL(temp_pool, svn_client_log3(apr_paths, &c_peg_rev,
        &c_start_rev, &c_end_rev, limit, discover_changed_paths?TRUE:FALSE,
        strict_node_history?TRUE:FALSE, py_svn_log_wrapper,
        (void*)callback, client->client, temp_pool));
#else
    RUN_SVN_WITH_POOL(temp_pool, svn_client_log2(apr_paths, &c_start_rev,
        &c_end_rev, limit, discover_changed_paths?TRUE:FALSE, strict_node_history?TRUE:FALSE,
        py_svn_log_wrapper, (void*)callback,
        client->client, temp_pool));
#endif

    apr_pool_destroy(temp_pool);
    Py_RETURN_NONE;
}

static PyObject *client_info(PyObject *self, PyObject *args, PyObject *kwargs)
{
    char *kwnames[] = {
        "path", "revision", "peg_revision", "depth",
        "fetch_excluded", "fetch_actual_only",
        NULL,
    };
    apr_pool_t *temp_pool;
    ClientObject *client = (ClientObject *)self;

    PyObject *py_path;
    const char *path;
    int depth = svn_depth_empty;
    bool fetch_excluded = false, fetch_actual_only = false;
    PyObject *revision = Py_None, *peg_revision = Py_None;
    svn_opt_revision_t c_peg_rev, c_rev;
    PyObject *entry_dict;
    svn_error_t *err;

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O|OOibb", kwnames,
                                     &py_path, &revision,
                                     &peg_revision, &depth,
                                     &fetch_excluded, &fetch_actual_only))
        return NULL;

    if (!to_opt_revision(revision, &c_rev))
        return NULL;
    if (!to_opt_revision(peg_revision, &c_peg_rev))
        return NULL;


    temp_pool = Pool(NULL);
    if (temp_pool == NULL)
        return NULL;

    path = py_object_to_svn_path_or_url(py_path, temp_pool);
    if (path == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    if (svn_path_is_url(path))
    {
      if (c_peg_rev.kind == svn_opt_revision_unspecified)
        c_peg_rev.kind = svn_opt_revision_head;
    }
    else
    {
        path = py_object_to_svn_abspath(py_path, temp_pool);
    }

#if ONLY_BEFORE_SVN(1, 5)
    if (depth != svn_depth_infinity && depth != svn_depth_empty) {
        PyErr_SetString(PyExc_NotImplementedError,
                        "depth can only be infinity or empty when built against svn < 1.5");
        return NULL;
    }
#endif

    entry_dict = PyDict_New();
    if (entry_dict == NULL) {
        apr_pool_destroy(temp_pool);
        return NULL;
    }

    Py_BEGIN_ALLOW_THREADS;
#if ONLY_SINCE_SVN(1, 7)
    /* FIXME: Support changelists */
	err = svn_client_info3(path, &c_peg_rev, &c_rev, depth, fetch_excluded?TRUE:FALSE,
						   fetch_actual_only?TRUE:FALSE, NULL,
						   info_receiver,
						   entry_dict,
						   client->client, temp_pool);
#elif ONLY_SINCE_SVN(1, 5)
    /* FIXME: Support changelists */
    err = svn_client_info2(path, &c_peg_rev, &c_rev, info_receiver, entry_dict,
                                                  depth, NULL,
                                                  client->client, temp_pool);
#else
    err = svn_client_info(path, &c_peg_rev, &c_rev,
                                                 info_receiver, entry_dict,
                                                 (depth == svn_depth_infinity),
                                                 client->client, temp_pool);
#endif
    Py_END_ALLOW_THREADS;

    if (err != NULL) {
        handle_svn_error(err);
        svn_error_clear(err);
        apr_pool_destroy(temp_pool);
        Py_DECREF(entry_dict);
        return NULL;
    }

    apr_pool_destroy(temp_pool);

    return entry_dict;
}

static PyMethodDef client_methods[] = {
    { "add", (PyCFunction)client_add, METH_VARARGS|METH_KEYWORDS,
        "S.add(path, recursive=True, force=False, no_ignore=False, no_autoprops=False)" },
    { "checkout", (PyCFunction)client_checkout, METH_VARARGS|METH_KEYWORDS,
        "S.checkout(url, path, rev=None, peg_rev=None, recurse=True, ignore_externals=False, allow_unver_obstructions=False)" },
	{ "export", (PyCFunction)client_export, METH_VARARGS|METH_KEYWORDS,
		"S.export(from, to, rev=None, peg_rev=None, recurse=True, ignore_externals=False, overwrite=False, native_eol=None)" },
    { "cat", (PyCFunction)client_cat, METH_VARARGS|METH_KEYWORDS,
        "S.cat(path, output_stream, revision=None, peg_revision=None)" },
    { "commit", (PyCFunction)client_commit, METH_VARARGS|METH_KEYWORDS, "S.commit(targets, recurse=True, keep_locks=True, revprops=None) -> (revnum, date, author)" },
    { "delete", client_delete, METH_VARARGS, "S.delete(paths, force=False)" },
    { "copy", (PyCFunction)client_copy, METH_VARARGS|METH_KEYWORDS, "S.copy(src_path, dest_path, srv_rev=None)" },
    { "propset", client_propset, METH_VARARGS, "S.propset(name, value, target, recurse=True, skip_checks=False)" },
    { "propget", client_propget, METH_VARARGS, "S.propget(name, target, peg_revision, revision=None, recurse=False) -> value" },
    { "proplist", (PyCFunction)client_proplist, METH_VARARGS|METH_KEYWORDS, "S.proplist(path, peg_revision, depth, revision=None)" },
    { "revproplist", (PyCFunction)client_revproplist, METH_VARARGS | METH_KEYWORDS, "S.revproplist(url, revision)" },
    { "resolve", client_resolve, METH_VARARGS, "S.resolve(path, depth, choice)" },
    { "update", (PyCFunction)client_update, METH_VARARGS|METH_KEYWORDS, "S.update(path, rev=None, recurse=True, ignore_externals=False) -> list of revnums" },
    { "list", (PyCFunction)client_list, METH_VARARGS|METH_KEYWORDS, "S.list(path, peg_revision, depth, dirents=ra.DIRENT_ALL, revision=None) -> list of directory entries" },
    { "diff", (PyCFunction)client_diff, METH_VARARGS|METH_KEYWORDS, "S.diff(rev1, rev2, path1=None, path2=None, relative_to_dir=None, diffopts=[], encoding=\"utf-8\", ignore_ancestry=True, no_diff_deleted=True, ignore_content_type=False) -> unified diff as a string" },
    { "mkdir", (PyCFunction)client_mkdir, METH_VARARGS|METH_KEYWORDS, "S.mkdir(paths, make_parents=False, revprops=None) -> (revnum, date, author)" },
    { "log", (PyCFunction)client_log, METH_VARARGS|METH_KEYWORDS,
        "S.log(callback, paths, start_rev=None, end_rev=None, limit=0, peg_revision=None, discover_changed_paths=False, strict_node_history=False, include_merged_revisions=False, revprops=None)" },
    { "info", (PyCFunction)client_info, METH_VARARGS|METH_KEYWORDS,
        "S.info(path, revision=None, peg_revision=None, depth=DEPTH_EMPTY) -> dict of info entries" },
    { NULL, }
};

static PyGetSetDef client_getset[] = {
    { "log_msg_func", client_get_log_msg_func, client_set_log_msg_func, NULL },
    { "notify_func", client_get_notify_func, client_set_notify_func, NULL },
    { "auth", NULL, client_set_auth, NULL },
    { "config", NULL, client_set_config, NULL },
    { NULL, }
};

static PyObject *get_default_ignores(PyObject *self)
{
	apr_array_header_t *patterns;
	apr_pool_t *pool;
	int i = 0;
	ConfigObject *configobj = (ConfigObject *)self;
	PyObject *ret;

	pool = Pool(NULL);
	if (pool == NULL)
		return NULL;
	RUN_SVN_WITH_POOL(pool, svn_wc_get_default_ignores(&patterns, configobj->config, pool));
	ret = PyList_New(patterns->nelts);
	for (i = 0; i < patterns->nelts; i++) {
		PyObject *item = PyBytes_FromString(APR_ARRAY_IDX(patterns, i, char *));
		if (item == NULL) {
			apr_pool_destroy(pool);
			Py_DECREF(item);
			Py_DECREF(ret);
			return NULL;
		}
        if (PyList_SetItem(ret, i, item) != 0) {
			apr_pool_destroy(pool);
			Py_DECREF(item);
			Py_DECREF(ret);
			return NULL;
		}
	}
	apr_pool_destroy(pool);
	return ret;
}

static PyMethodDef config_methods[] = {
    { "get_default_ignores", (PyCFunction)get_default_ignores, METH_NOARGS, NULL },
    { NULL }
};


static void config_dealloc(PyObject *obj)
{
    apr_pool_t *pool = ((ConfigObject *)obj)->pool;
    if (pool != NULL)
        apr_pool_destroy(pool);
    PyObject_Del(obj);
}

PyTypeObject Config_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "client.Config", /*    const char *tp_name;  For printing, in format "<module>.<name>" */
    sizeof(ConfigObject),  /*  tp_basicsize    */
    0,  /*    tp_itemsize;  For allocation */

    /* Methods to implement standard operations */

    (destructor)config_dealloc, /*    destructor tp_dealloc;    */
    0, /*    printfunc tp_print;    */
    NULL, /*    getattrfunc tp_getattr;    */
    NULL, /*    setattrfunc tp_setattr;    */
    NULL, /*    cmpfunc tp_compare;    */
    NULL, /*    reprfunc tp_repr;    */

    /* Method suites for standard classes */

    NULL, /*    PyNumberMethods *tp_as_number;    */
    NULL, /*    PySequenceMethods *tp_as_sequence;    */
    NULL, /*    PyMappingMethods *tp_as_mapping;    */

    /* More standard operations (here for binary compatibility) */

    NULL, /*    hashfunc tp_hash;    */
    NULL, /*    ternaryfunc tp_call;    */
    NULL, /*    reprfunc tp_str;    */
    NULL, /*    getattrofunc tp_getattro;    */
    NULL, /*    setattrofunc tp_setattro;    */

    /* Functions to access object as input/output buffer */
    NULL, /*    PyBufferProcs *tp_as_buffer;    */

    /* Flags to define presence of optional/expanded features */
    0, /*    long tp_flags;    */

    NULL, /*    const char *tp_doc;  Documentation string */

    /* Assigned meaning in release 2.0 */
    /* call function for all accessible objects */
    NULL, /*    traverseproc tp_traverse;    */

    /* delete references to contained objects */
    NULL, /*    inquiry tp_clear;    */

    /* Assigned meaning in release 2.1 */
    /* rich comparisons */
    NULL, /*    richcmpfunc tp_richcompare;    */

    /* weak reference enabler */
    0, /*    Py_ssize_t tp_weaklistoffset;    */

    /* Added in release 2.2 */
    /* Iterators */
    NULL, /*    getiterfunc tp_iter;    */
    NULL, /*    iternextfunc tp_iternext;    */

    /* Attribute descriptor and subclassing stuff */
    config_methods, /*    struct PyMethodDef *tp_methods;    */
};

static void configitem_dealloc(PyObject *self)
{
    ConfigItemObject *item = (ConfigItemObject *)self;

    Py_XDECREF(item->parent);
    PyObject_Del(item);
}

PyTypeObject ConfigItem_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "client.ConfigItem", /*    const char *tp_name;  For printing, in format "<module>.<name>" */
    sizeof(ConfigItemObject),
    0,/*    Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

    /* Methods to implement standard operations */

    (destructor)configitem_dealloc, /*    destructor tp_dealloc;    */
};

static void info_dealloc(PyObject *self)
{
    apr_pool_t *pool = ((InfoObject *)self)->pool;
    if (pool != NULL)
        apr_pool_destroy(pool);
    PyObject_Del(self);
}

static PyMemberDef info_members[] = {
    { "url", T_STRING, offsetof(InfoObject, info.URL), READONLY,
        "Where the item lives in the repository." },
    { "revision", T_LONG, offsetof(InfoObject, info.rev), READONLY,
        "The revision of the object.", },
    { "kind", T_INT, offsetof(InfoObject, info.kind), READONLY,
        "The node's kind.", },
    { "repos_root_url", T_STRING, offsetof(InfoObject, info.repos_root_URL), READONLY,
        "The root URL of the repository." },
    { "repos_uuid", T_STRING, offsetof(InfoObject, info.repos_UUID), READONLY,
        "The repository's UUID." },
    { "last_changed_revision", T_LONG, offsetof(InfoObject, info.last_changed_rev), READONLY,
        "The last revision in which this object changed.", },
    { "last_changed_date", T_LONG, offsetof(InfoObject, info.last_changed_date), READONLY,
        "The date of the last_changed_revision." },
    { "last_changed_author", T_STRING, offsetof(InfoObject, info.last_changed_author), READONLY,
        "The author of the last_changed_revision." },
    { "wc_info", T_OBJECT, offsetof(InfoObject, wc_info), READONLY,
        "Possible information about the working copy, None if not valid." },
    { NULL, }
};

static PyObject *info_get_size(PyObject *_self, void *closure)
{
    InfoObject *self = (InfoObject *)_self;
    if (self->info.size == SVN_WC_ENTRY_WORKING_SIZE_UNKNOWN)
        Py_RETURN_NONE;
    return PyLong_FromLong(self->info.size);
}

static PyGetSetDef info_getsetters[] = {
    { "size", info_get_size, NULL, "The size of the file in the repository.", },
    { NULL }
};

PyTypeObject Info_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "client.Info", /*   const char *tp_name;  For printing, in format "<module>.<name>" */
    sizeof(InfoObject),
    0,/*    Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

    /* Methods to implement standard operations */

    info_dealloc, /*    destructor tp_dealloc;  */
    0, /*    printfunc tp_print; */
    NULL, /*    getattrfunc tp_getattr; */
    NULL, /*    setattrfunc tp_setattr; */
    NULL, /*    cmpfunc tp_compare; */
    NULL, /*    reprfunc tp_repr;   */

    /* Method suites for standard classes */

    NULL, /*    PyNumberMethods *tp_as_number;  */
    NULL, /*    PySequenceMethods *tp_as_sequence;  */
    NULL, /*    PyMappingMethods *tp_as_mapping;    */

    /* More standard operations (here for binary compatibility) */

    NULL, /*    hashfunc tp_hash;   */
    NULL, /*    ternaryfunc tp_call;    */
    NULL, /*    reprfunc tp_str;    */
    NULL, /*    getattrofunc tp_getattro;   */
    NULL, /*    setattrofunc tp_setattro;   */

    /* Functions to access object as input/output buffer */
    NULL, /*    PyBufferProcs *tp_as_buffer;    */

    /* Flags to define presence of optional/expanded features */
    0, /*   long tp_flags;  */

    NULL, /*    const char *tp_doc;  Documentation string */

    /* Assigned meaning in release 2.0 */
    /* call function for all accessible objects */
    NULL, /*    traverseproc tp_traverse;   */

    /* delete references to contained objects */
    NULL, /*    inquiry tp_clear;   */

    /* Assigned meaning in release 2.1 */
    /* rich comparisons */
    NULL, /*    richcmpfunc tp_richcompare; */

    /* weak reference enabler */
    0, /*   Py_ssize_t tp_weaklistoffset;   */

    /* Added in release 2.2 */
    /* Iterators */
    NULL, /*    getiterfunc tp_iter;    */
    NULL, /*    iternextfunc tp_iternext;   */

    /* Attribute descriptor and subclassing stuff */
    NULL, /*    struct PyMethodDef *tp_methods; */
    info_members, /*    struct PyMemberDef *tp_members; */
    info_getsetters, /* struct PyGetSetDef *tp_getsetters; */
};

static PyMemberDef wc_info_members[] = {
    { "schedule", T_INT, offsetof(WCInfoObject, info.schedule), READONLY,
        "" },
    { "copyfrom_url", T_STRING, offsetof(WCInfoObject, info.copyfrom_url), READONLY,
        "" },
    { "copyfrom_rev", T_LONG, offsetof(WCInfoObject, info.copyfrom_rev), READONLY,
        "" },
    /* TODO add support for checksum */
    /* TODO add support for conflicts */
#if ONLY_SINCE_SVN(1, 7)
    { "changelist", T_STRING, offsetof(WCInfoObject, info.changelist), READONLY,
        "" },
    { "recorded_size", T_PYSSIZET, offsetof(WCInfoObject, info.recorded_size), READONLY,
        "" },
    { "recorded_time", T_LONG, offsetof(WCInfoObject, info.recorded_time), READONLY,
        "" },
    { "wcroot_abspath", T_STRING, offsetof(WCInfoObject, info.wcroot_abspath), READONLY,
        "" },
#else
#if ONLY_SINCE_SVN(1, 5)
    { "depth", T_INT, offsetof(WCInfoObject, info.depth), READONLY,
        "" },
#endif
    { "recorded_size", T_PYSSIZET, offsetof(InfoObject, info.WORKING_SIZE), READONLY,
        "The size of the file in the repository.", },
    { "text_time", T_LONG, offsetof(WCInfoObject, info.text_time), READONLY,
        "" },
    { "prop_time", T_LONG, offsetof(WCInfoObject, info.prop_time), READONLY,
        "" },
#endif
    { NULL, }
};

static void wcinfo_dealloc(PyObject *self)
{
	PyObject_Del(self);
}

PyTypeObject WCInfo_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "client.Info", /*   const char *tp_name;  For printing, in format "<module>.<name>" */
    sizeof(WCInfoObject),
    0,/*    Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

    /* Methods to implement standard operations */

    wcinfo_dealloc, /*    destructor tp_dealloc;  */
    0, /*    printfunc tp_print; */
    NULL, /*    getattrfunc tp_getattr; */
    NULL, /*    setattrfunc tp_setattr; */
    NULL, /*    cmpfunc tp_compare; */
    NULL, /*    reprfunc tp_repr;   */

    /* Method suites for standard classes */

    NULL, /*    PyNumberMethods *tp_as_number;  */
    NULL, /*    PySequenceMethods *tp_as_sequence;  */
    NULL, /*    PyMappingMethods *tp_as_mapping;    */

    /* More standard operations (here for binary compatibility) */

    NULL, /*    hashfunc tp_hash;   */
    NULL, /*    ternaryfunc tp_call;    */
    NULL, /*    reprfunc tp_str;    */
    NULL, /*    getattrofunc tp_getattro;   */
    NULL, /*    setattrofunc tp_setattro;   */

    /* Functions to access object as input/output buffer */
    NULL, /*    PyBufferProcs *tp_as_buffer;    */

    /* Flags to define presence of optional/expanded features */
    0, /*   long tp_flags;  */

    NULL, /*    const char *tp_doc;  Documentation string */

    /* Assigned meaning in release 2.0 */
    /* call function for all accessible objects */
    NULL, /*    traverseproc tp_traverse;   */

    /* delete references to contained objects */
    NULL, /*    inquiry tp_clear;   */

    /* Assigned meaning in release 2.1 */
    /* rich comparisons */
    NULL, /*    richcmpfunc tp_richcompare; */

    /* weak reference enabler */
    0, /*   Py_ssize_t tp_weaklistoffset;   */

    /* Added in release 2.2 */
    /* Iterators */
    NULL, /*    getiterfunc tp_iter;    */
    NULL, /*    iternextfunc tp_iternext;   */

    /* Attribute descriptor and subclassing stuff */
    NULL, /*    struct PyMethodDef *tp_methods; */
    wc_info_members, /*    struct PyMemberDef *tp_members; */

};

PyTypeObject Client_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    /*    PyObject_VAR_HEAD    */
    "client.Client", /*    const char *tp_name;  For printing, in format "<module>.<name>" */
    sizeof(ClientObject),
    0,/*    Py_ssize_t tp_basicsize, tp_itemsize;  For allocation */

    /* Methods to implement standard operations */

    client_dealloc, /*    destructor tp_dealloc;    */
    0, /*    printfunc tp_print;    */
    NULL, /*    getattrfunc tp_getattr;    */
    NULL, /*    setattrfunc tp_setattr;    */
    NULL, /*    cmpfunc tp_compare;    */
    NULL, /*    reprfunc tp_repr;    */

    /* Method suites for standard classes */

    NULL, /*    PyNumberMethods *tp_as_number;    */
    NULL, /*    PySequenceMethods *tp_as_sequence;    */
    NULL, /*    PyMappingMethods *tp_as_mapping;    */

    /* More standard operations (here for binary compatibility) */

    NULL, /*    hashfunc tp_hash;    */
    NULL, /*    ternaryfunc tp_call;    */
    NULL, /*    reprfunc tp_str;    */
    NULL, /*    getattrofunc tp_getattro;    */
    NULL, /*    setattrofunc tp_setattro;    */

    /* Functions to access object as input/output buffer */
    NULL, /*    PyBufferProcs *tp_as_buffer;    */

    /* Flags to define presence of optional/expanded features */
    0, /*    long tp_flags;    */

    "Subversion client", /*    const char *tp_doc;  Documentation string */

    /* Assigned meaning in release 2.0 */
    /* call function for all accessible objects */
    NULL, /*    traverseproc tp_traverse;    */

    /* delete references to contained objects */
    NULL, /*    inquiry tp_clear;    */

    /* Assigned meaning in release 2.1 */
    /* rich comparisons */
    NULL, /*    richcmpfunc tp_richcompare;    */

    /* weak reference enabler */
    0, /*    Py_ssize_t tp_weaklistoffset;    */

    /* Added in release 2.2 */
    /* Iterators */
    NULL, /*    getiterfunc tp_iter;    */
    NULL, /*    iternextfunc tp_iternext;    */

    /* Attribute descriptor and subclassing stuff */
    client_methods, /*    struct PyMethodDef *tp_methods;    */
    NULL, /*    struct PyMemberDef *tp_members;    */
    client_getset, /*    struct PyGetSetDef *tp_getset;    */
    NULL, /*    struct _typeobject *tp_base;    */
    NULL, /*    PyObject *tp_dict;    */
    NULL, /*    descrgetfunc tp_descr_get;    */
    NULL, /*    descrsetfunc tp_descr_set;    */
    0, /*    Py_ssize_t tp_dictoffset;    */
    NULL, /*    initproc tp_init;    */
    NULL, /*    allocfunc tp_alloc;    */
    client_new, /*    newfunc tp_new;    */

};

static PyObject *get_config(PyObject *self, PyObject *args)
{
    char *config_dir = NULL;
    ConfigObject *data;

    if (!PyArg_ParseTuple(args, "|z", &config_dir))
        return NULL;

    data = PyObject_New(ConfigObject, &Config_Type);
    if (data == NULL)
        return NULL;

    data->pool = Pool(NULL);
    if (data->pool == NULL) {
        PyObject_Del(data);
        return NULL;
    }

    RUN_SVN_WITH_POOL(data->pool,
                      svn_config_get_config(&data->config, config_dir, data->pool));

    return (PyObject *)data;
}

/**
 * Get runtime libsvn_wc version information.
 *
 * :return: tuple with major, minor, patch version number and tag.
 */
static PyObject *version(PyObject *self)
{
	const svn_version_t *ver = svn_client_version();
	return Py_BuildValue("(iiis)", ver->major, ver->minor,
						 ver->patch, ver->tag);
}

SVN_VERSION_DEFINE(svn_api_version);

/**
 * Get compile-time libsvn_wc version information.
 *
 * :return: tuple with major, minor, patch version number and tag.
 */
static PyObject *api_version(PyObject *self)
{
	const svn_version_t *ver = &svn_api_version;
	return Py_BuildValue("(iiis)", ver->major, ver->minor,
						 ver->patch, ver->tag);
}



static PyMethodDef client_mod_methods[] = {
	{ "get_config", get_config, METH_VARARGS, "get_config(config_dir=None) -> config" },
	{ "api_version", (PyCFunction)api_version, METH_NOARGS,
		"api_version() -> (major, minor, patch, tag)\n\n"
		"Version of libsvn_client Subvertpy was compiled against."
	},
	{ "version", (PyCFunction)version, METH_NOARGS,
		"version() -> (major, minor, patch, tag)\n\n"
		"Version of libsvn_wc currently used."
	},
	{ NULL }
};

static PyObject *
moduleinit(void)
{
	PyObject *mod;

	if (PyType_Ready(&Client_Type) < 0)
		return NULL;

	if (PyType_Ready(&Config_Type) < 0)
		return NULL;

	if (PyType_Ready(&ConfigItem_Type) < 0)
		return NULL;

	if (PyType_Ready(&Info_Type) < 0)
		return NULL;

	if (PyType_Ready(&WCInfo_Type) < 0)
		return NULL;

	/* Make sure APR is initialized */
	apr_initialize();

#if PY_MAJOR_VERSION >= 3
	static struct PyModuleDef moduledef = {
	  PyModuleDef_HEAD_INIT,
	  "client",         /* m_name */
	  "Client methods",            /* m_doc */
	  -1,              /* m_size */
	  client_mod_methods, /* m_methods */
	  NULL,            /* m_reload */
	  NULL,            /* m_traverse */
	  NULL,            /* m_clear*/
	  NULL,            /* m_free */
	};
	mod = PyModule_Create(&moduledef);
#else
	mod = Py_InitModule3("subvertpy.client", client_mod_methods, "Client methods");
#endif
	if (mod == NULL)
		return NULL;

	Py_INCREF(&Client_Type);
	PyModule_AddObject(mod, "Client", (PyObject *)&Client_Type);

	PyModule_AddObject(mod, "depth_empty",
					   (PyObject *)PyLong_FromLong(svn_depth_empty));
	PyModule_AddObject(mod, "depth_files",
					   (PyObject *)PyLong_FromLong(svn_depth_files));
	PyModule_AddObject(mod, "depth_immediates",
					   (PyObject *)PyLong_FromLong(svn_depth_immediates));
	PyModule_AddObject(mod, "depth_infinity",
					   (PyObject *)PyLong_FromLong(svn_depth_infinity));

	Py_INCREF(&Config_Type);
	PyModule_AddObject(mod, "Config", (PyObject *)&Config_Type);

	return mod;
}

#if PY_MAJOR_VERSION >= 3
PyMODINIT_FUNC
PyInit_client(void)
{
	return moduleinit();
}
#else
PyMODINIT_FUNC
initclient(void)
{
	moduleinit();
}
#endif
