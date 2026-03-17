/* DO NOT EDIT: automatically built by dist/s_include. */
#ifndef	_xa_ext_h_
#define	_xa_ext_h_

#if defined(__cplusplus)
extern "C" {
#endif

int __db_rmid_to_env __P((int, ENV **));
int __db_xid_to_txn __P((ENV *, XID *, TXN_DETAIL **));
void __db_map_rmid __P((int, ENV *));
int __db_unmap_rmid __P((int));
void __db_unmap_xid __P((ENV *, XID *, size_t));

#if defined(__cplusplus)
}
#endif
#endif /* !_xa_ext_h_ */
