CREATE TABLE gh.workflow_jobs (
    `id` UInt64,
    `run_id` UInt64,
    `workflow_name` LowCardinality(String),
    `head_branch` LowCardinality(String),
    `run_url` String,
    `run_attempt` UInt16,
    `node_id` String,
    `head_sha` String,
    `url` String,
    `html_url` String,
    `status` Enum8('queued' = 1, 'in_progress' = 2, 'completed' = 3, 'waiting' = 4),
    `conclusion` LowCardinality(String),
    `started_at` DateTime,
    `completed_at` DateTime,
    `name` LowCardinality(String),
    `steps` UInt16,
    `check_run_url` String,
    `labels` Array(LowCardinality(String)),
    `runner_id` UInt64,
    `runner_name` String,
    `runner_group_id` UInt64,
    `runner_group_name` LowCardinality(String),
    `repository` LowCardinality(String),
    `updated_at` DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(started_at)
ORDER BY (id, updated_at);


CREATE TABLE gh.workflow_job_steps (
   wf_id UInt64,
   wf_run_id UInt64,
   wf_started_at DateTime,
   name LowCardinality(String),
   status LowCardinality(String),
   conclusion LowCardinality(String),
   number UInt16,
   started_at DateTime,
   completed_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(wf_started_at)
ORDER BY (wf_id, number);


CREATE TABLE gh.pull_request (
    id UInt64,
    action LowCardinality(String),
    state LowCardinality(String),
    number UInt64,
    url String,
    html_url String,
    user_login LowCardinality(String),
    labels Array(LowCardinality(String)),
    head_sha String,
    head_ref String,
    base_sha String,
    base_ref String,
    merge_commit_sha String,
    draft bool,
    merged bool,
    created_at DateTime,
    updated_at DateTime,
    closed_at DateTime,
    merged_at DateTime

)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(created_at)
ORDER BY (number, action);
