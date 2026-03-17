#include "db_config.h"

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_am.h"
#include "dbinc_auto/repmgr_auto.h"

/*
 * __repmgr_member_recover --
 *	Recovery function for member.
 *
 * PUBLIC: int __repmgr_member_recover
 * PUBLIC:   __P((ENV *, DBT *, DB_LSN *, db_recops, void *));
 */
int
__repmgr_member_recover(env, dbtp, lsnp, op, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops op;
	void *info;
{
	__repmgr_member_args *argp;
	int ret;

	COMPQUIET(info, NULL);
	COMPQUIET(op, DB_TXN_APPLY);

	REC_PRINT(__repmgr_member_print);
	REC_NOOP_INTRO(__repmgr_member_read);

	/*
	 * The annotation log record describes the update in enough detail for
	 * us to be able to optimize our tracking of it at clients sites.
	 * However, for now we just simply reread the whole (small) database
	 * each time, since changes happen so seldom (and we need to have the
	 * code for reading the whole thing anyway, for other cases).
	 */
	env->rep_handle->gmdb_dirty = TRUE;

	*lsnp = argp->prev_lsn;
	ret = 0;

	REC_NOOP_CLOSE;
}
