typedef struct git_commit git_commit;
typedef struct git_annotated_commit git_annotated_commit;
typedef struct git_config git_config;
typedef struct git_index git_index;
typedef struct git_index_conflict_iterator git_index_conflict_iterator;
typedef struct git_object git_object;
typedef struct git_refspec git_refspec;
typedef struct git_remote git_remote;
typedef struct git_remote_callbacks git_remote_callbacks;
typedef struct git_repository git_repository;
typedef struct git_submodule git_submodule;
typedef struct git_transport git_transport;
typedef struct git_tree git_tree;
typedef struct git_packbuilder git_packbuilder;
typedef struct git_transaction git_transaction;
typedef struct git_reflog git_reflog;

typedef int64_t git_off_t;
typedef int64_t git_time_t;

typedef enum {
	GIT_REFERENCE_INVALID  = 0,
	GIT_REFERENCE_DIRECT   = 1,
	GIT_REFERENCE_SYMBOLIC = 2,
	GIT_REFERENCE_ALL      = 3,
} git_reference_t;

typedef struct git_time {
	git_time_t time;
	int offset;
	char sign;
} git_time;

typedef struct git_signature {
	char *name;
	char *email;
	git_time when;
} git_signature;

typedef enum git_cert_t {
	GIT_CERT_NONE,
	GIT_CERT_X509,
	GIT_CERT_HOSTKEY_LIBSSH2,
	GIT_CERT_STRARRAY,
} git_cert_t;

typedef struct {
	git_cert_t cert_type;
} git_cert;

typedef int (*git_transport_message_cb)(const char *str, int len, void *payload);
typedef int (*git_transport_certificate_check_cb)(git_cert *cert, int valid, const char *host, void *payload);

typedef enum {
	GIT_SUBMODULE_IGNORE_UNSPECIFIED  = -1,

	GIT_SUBMODULE_IGNORE_NONE      = 1,
	GIT_SUBMODULE_IGNORE_UNTRACKED = 2,
	GIT_SUBMODULE_IGNORE_DIRTY     = 3,
	GIT_SUBMODULE_IGNORE_ALL       = 4,
} git_submodule_ignore_t;

typedef enum {
	GIT_OBJECT_ANY       = ...,
	GIT_OBJECT_INVALID   = ...,
	GIT_OBJECT_COMMIT    = ...,
	GIT_OBJECT_TREE      = ...,
	GIT_OBJECT_BLOB      = ...,
	GIT_OBJECT_TAG       = ...,
	GIT_OBJECT_OFS_DELTA = ...,
	GIT_OBJECT_REF_DELTA = ...,
} git_object_t;
