CREATE TABLE `/Root/quotas_browsers_relation` (
    quota_id	Utf8	NOT NULL,
    browser_id	Utf8	NOT NULL,
    id	Utf8,
    created_at	Timestamp,
    deleted_at	Timestamp,
    primary key (quota_id, browser_id)
);

CREATE TABLE `/Root/browsers` (
    id	Utf8	NOT NULL,	
	name	Utf8,	
	version	Utf8,	
	group	Utf8,	
	created_at	Timestamp,	
	deleted_at	Timestamp,	
	description	Utf8,
    primary key (id)
);

CREATE TABLE `/Root/browser_groups` (
    name	Utf8	NOT NULL,	
	platform	Utf8,	
	sessions_per_agent_limit	Uint32	,
	cpu_cores_per_session	Double	,
	ramdrive_gb_per_session	Double	,
	ram_gb_per_session	Double	,
	ramdrive_size_gb	Double	,
	session_request_timeout_ms	Uint32	,
	browser_platform	Utf8	,
	service_startup_timeout_ms	Uint32	,
	session_attempt_timeout_ms	Uint32,
	primary key (name)
);

CREATE TABLE `/Root/quota` (
    name	Utf8	NOT NULL,	
	created_at	Timestamp	,
	owner	Utf8	,
	agents_max_limit	Uint32	,
	agent_kill_timeout_ms	Uint32	,
	agent_queue_time_limit_ms	Uint32	,
	agent_secret_id	Utf8	,
	id	Utf8	,
    primary key(name)
);
