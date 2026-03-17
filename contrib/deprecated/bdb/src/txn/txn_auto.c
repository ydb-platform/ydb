/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"
#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/txn.h"

DB_LOG_RECSPEC __txn_regop_42_desc[] = {
	{LOGREC_ARG, SSZ(__txn_regop_42_args, opcode), "opcode", "%lu"},
	{LOGREC_TIME, SSZ(__txn_regop_42_args, timestamp), "timestamp", ""},
	{LOGREC_LOCKS, SSZ(__txn_regop_42_args, locks), "locks", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_regop_desc[] = {
	{LOGREC_ARG, SSZ(__txn_regop_args, opcode), "opcode", "%lu"},
	{LOGREC_TIME, SSZ(__txn_regop_args, timestamp), "timestamp", ""},
	{LOGREC_ARG, SSZ(__txn_regop_args, envid), "envid", "%lu"},
	{LOGREC_LOCKS, SSZ(__txn_regop_args, locks), "locks", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_ckp_42_desc[] = {
	{LOGREC_POINTER, SSZ(__txn_ckp_42_args, ckp_lsn), "ckp_lsn", ""},
	{LOGREC_POINTER, SSZ(__txn_ckp_42_args, last_ckp), "last_ckp", ""},
	{LOGREC_TIME, SSZ(__txn_ckp_42_args, timestamp), "timestamp", ""},
	{LOGREC_ARG, SSZ(__txn_ckp_42_args, rep_gen), "rep_gen", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_ckp_desc[] = {
	{LOGREC_POINTER, SSZ(__txn_ckp_args, ckp_lsn), "ckp_lsn", ""},
	{LOGREC_POINTER, SSZ(__txn_ckp_args, last_ckp), "last_ckp", ""},
	{LOGREC_TIME, SSZ(__txn_ckp_args, timestamp), "timestamp", ""},
	{LOGREC_ARG, SSZ(__txn_ckp_args, envid), "envid", "%lu"},
	{LOGREC_ARG, SSZ(__txn_ckp_args, spare), "spare", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_child_desc[] = {
	{LOGREC_ARG, SSZ(__txn_child_args, child), "child", "%lx"},
	{LOGREC_POINTER, SSZ(__txn_child_args, c_lsn), "c_lsn", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_xa_regop_42_desc[] = {
	{LOGREC_ARG, SSZ(__txn_xa_regop_42_args, opcode), "opcode", "%lu"},
	{LOGREC_DBT, SSZ(__txn_xa_regop_42_args, xid), "xid", ""},
	{LOGREC_ARG, SSZ(__txn_xa_regop_42_args, formatID), "formatID", "%ld"},
	{LOGREC_ARG, SSZ(__txn_xa_regop_42_args, gtrid), "gtrid", "%lu"},
	{LOGREC_ARG, SSZ(__txn_xa_regop_42_args, bqual), "bqual", "%lu"},
	{LOGREC_POINTER, SSZ(__txn_xa_regop_42_args, begin_lsn), "begin_lsn", ""},
	{LOGREC_LOCKS, SSZ(__txn_xa_regop_42_args, locks), "locks", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_prepare_desc[] = {
	{LOGREC_ARG, SSZ(__txn_prepare_args, opcode), "opcode", "%lu"},
	{LOGREC_DBT, SSZ(__txn_prepare_args, gid), "gid", ""},
	{LOGREC_POINTER, SSZ(__txn_prepare_args, begin_lsn), "begin_lsn", ""},
	{LOGREC_LOCKS, SSZ(__txn_prepare_args, locks), "locks", ""},
	{LOGREC_Done, 0, "", ""}
};
DB_LOG_RECSPEC __txn_recycle_desc[] = {
	{LOGREC_ARG, SSZ(__txn_recycle_args, min), "min", "%lu"},
	{LOGREC_ARG, SSZ(__txn_recycle_args, max), "max", "%lu"},
	{LOGREC_Done, 0, "", ""}
};
/*
 * PUBLIC: int __txn_init_recover __P((ENV *, DB_DISTAB *));
 */
int
__txn_init_recover(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_regop_recover, DB___txn_regop)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_ckp_recover, DB___txn_ckp)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_child_recover, DB___txn_child)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_prepare_recover, DB___txn_prepare)) != 0)
		return (ret);
	if ((ret = __db_add_recovery_int(env, dtabp,
	    __txn_recycle_recover, DB___txn_recycle)) != 0)
		return (ret);
	return (0);
}
