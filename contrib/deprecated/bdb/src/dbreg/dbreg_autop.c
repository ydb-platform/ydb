/* Do not edit: automatically built by gen_rec.awk. */

#include "db_config.h"

#include "db_int.h"
#include "dbinc/crypto.h"
#include "dbinc/db_page.h"
#include "dbinc/db_dispatch.h"
#include "dbinc/db_am.h"
#include "dbinc/txn.h"

/*
 * PUBLIC: int __dbreg_register_print __P((ENV *, DBT *, DB_LSN *,
 * PUBLIC:     db_recops, void *));
 */
int
__dbreg_register_print(env, dbtp, lsnp, notused2, info)
	ENV *env;
	DBT *dbtp;
	DB_LSN *lsnp;
	db_recops notused2;
	void *info;
{
	COMPQUIET(notused2, DB_TXN_PRINT);

	return (__log_print_record(env, dbtp, lsnp, "__dbreg_register", __dbreg_register_desc, info));
}

/*
 * PUBLIC: int __dbreg_init_print __P((ENV *, DB_DISTAB *));
 */
int
__dbreg_init_print(env, dtabp)
	ENV *env;
	DB_DISTAB *dtabp;
{
	int ret;

	if ((ret = __db_add_recovery_int(env, dtabp,
	    __dbreg_register_print, DB___dbreg_register)) != 0)
		return (ret);
	return (0);
}
