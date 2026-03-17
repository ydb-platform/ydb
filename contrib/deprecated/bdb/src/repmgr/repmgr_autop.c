/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#ifdef HAVE_REPLICATION_THREADS
#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc_auto/repmgr_auto.h"

/*
 * PUBLIC: int __repmgr_member_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__repmgr_member_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__repmgr_member", __repmgr_member_desc, info));
}

/*
 * PUBLIC: int __repmgr_init_print __P((ENV *, DB_DISTAB *));
 */
int
__repmgr_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __repmgr_member_print, DB___repmgr_member)) != 0)
		return (ret);
	return (0);
}
#endif /* HAVE_REPLICATION_THREADS */
