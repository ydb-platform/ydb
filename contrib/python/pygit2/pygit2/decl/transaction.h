int git_transaction_new(git_transaction **out, git_repository *repo);
int git_transaction_lock_ref(git_transaction *tx, const char *refname);
int git_transaction_set_target(git_transaction *tx, const char *refname, const git_oid *target, const git_signature *sig, const char *msg);
int git_transaction_set_symbolic_target(git_transaction *tx, const char *refname, const char *target, const git_signature *sig, const char *msg);
int git_transaction_set_reflog(git_transaction *tx, const char *refname, const git_reflog *reflog);
int git_transaction_remove(git_transaction *tx, const char *refname);
int git_transaction_commit(git_transaction *tx);
void git_transaction_free(git_transaction *tx);
