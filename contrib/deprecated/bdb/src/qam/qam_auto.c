/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/qam.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __qam_incfirst_desc[] = {
	{LOGREC_DB, SSZ(__qam_incfirst_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__qam_incfirst_args, recno), "recno", "%lu"},
	{LOGREC_ARG, SSZ(__qam_incfirst_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __qam_mvptr_desc[] = {
	{LOGREC_ARG, SSZ(__qam_mvptr_args, opcode), "opcode", "%lu"},
	{LOGREC_DB, SSZ(__qam_mvptr_args, fileid), "fileid", ""},
	{LOGREC_ARG, SSZ(__qam_mvptr_args, old_first), "old_first", "%lu"},
	{LOGREC_ARG, SSZ(__qam_mvptr_args, new_first), "new_first", "%lu"},
	{LOGREC_ARG, SSZ(__qam_mvptr_args, old_cur), "old_cur", "%lu"},
	{LOGREC_ARG, SSZ(__qam_mvptr_args, new_cur), "new_cur", "%lu"},
	{LOGREC_POINTER, SSZ(__qam_mvptr_args, metalsn), "metalsn", ""},
	{LOGREC_ARG, SSZ(__qam_mvptr_args, meta_pgno), "meta_pgno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __qam_del_desc[] = {
	{LOGREC_DB, SSZ(__qam_del_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__qam_del_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__qam_del_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__qam_del_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__qam_del_args, recno), "recno", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __qam_add_desc[] = {
	{LOGREC_DB, SSZ(__qam_add_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__qam_add_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__qam_add_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__qam_add_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__qam_add_args, recno), "recno", "%lu"},
	{LOGREC_DBT, SSZ(__qam_add_args, data), "data", ""},
	{LOGREC_ARG, SSZ(__qam_add_args, vflag), "vflag", "%lu"},
	{LOGREC_DBT, SSZ(__qam_add_args, olddata), "olddata", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __qam_delext_desc[] = {
	{LOGREC_DB, SSZ(__qam_delext_args, fileid), "fileid", ""},
	{LOGREC_POINTER, SSZ(__qam_delext_args, lsn), "lsn", ""},
	{LOGREC_ARG, SSZ(__qam_delext_args, pgno), "pgno", "%lu"},
	{LOGREC_ARG, SSZ(__qam_delext_args, indx), "indx", "%lu"},
	{LOGREC_ARG, SSZ(__qam_delext_args, recno), "recno", "%lu"},
	{LOGREC_DBT, SSZ(__qam_delext_args, data), "data", ""},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __qam_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__qam_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_incfirst_recover, DB___qam_incfirst)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_mvptr_recover, DB___qam_mvptr)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_del_recover, DB___qam_del)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_add_recover, DB___qam_add)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __qam_delext_recover, DB___qam_delext)) != 0)
		return (ret);
	return (0);
}
