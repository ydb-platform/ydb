#define GIT_REVERT_OPTIONS_VERSION ...

typedef struct {
	unsigned int version;
	unsigned int mainline;
	git_merge_options merge_opts;
	git_checkout_options checkout_opts;
} git_revert_options;

int git_revert(
	git_repository *repo,
	git_commit *commit,
	const git_revert_options *given_opts);

int git_revert_commit(
	git_index **out,
	git_repository *repo,
	git_commit *revert_commit,
	git_commit *our_commit,
	unsigned int mainline,
	const git_merge_options *merge_options);
