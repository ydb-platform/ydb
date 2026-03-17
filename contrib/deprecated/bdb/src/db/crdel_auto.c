/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __crdel_metasub_desc[] = {
	{LOGREC_DB, SSZ(__crdel_metasub_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__crdel_metasub_args, pgno), "pgno", "%lu"},
	{LOGREC_PGDBT, SSZ(__crdel_metasub_args, page), "page", ""},
	{LOGREC_POINTER, SSZ(__crdel_metasub_args, lsn), "lsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __crdel_inmem_create_desc[] = {
	{LOGREC_ARG, SSZ(__crdel_inmem_create_args, fileid), "fileid", "%ld"},
	{LOGREC_DBT, SSZ(__crdel_inmem_create_args, name), "name", ""},
	{LOGREC_DBT, SSZ(__crdel_inmem_create_args, fid), "fid", ""},
	{LOGREC_ARG, SSZ(__crdel_inmem_create_args, pgsize), "pgsize", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __crdel_inmem_rename_desc[] = {
	{LOGREC_DBT, SSZ(__crdel_inmem_rename_args, oldname), "oldname", ""},
	{LOGREC_DBT, SSZ(__crdel_inmem_rename_args, newname), "newname", ""},
	{LOGREC_DBT, SSZ(__crdel_inmem_rename_args, fid), "fid", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __crdel_inmem_remove_desc[] = {
	{LOGREC_DBT, SSZ(__crdel_inmem_remove_args, name), "name", ""},
	{LOGREC_DBT, SSZ(__crdel_inmem_remove_args, fid), "fid", ""},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __crdel_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__crdel_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_metasub_recover, DB___crdel_metasub)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_create_recover, DB___crdel_inmem_create)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_rename_recover, DB___crdel_inmem_rename)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __crdel_inmem_remove_recover, DB___crdel_inmem_remove)) != 0)
		return (ret);
	return (0);
}
