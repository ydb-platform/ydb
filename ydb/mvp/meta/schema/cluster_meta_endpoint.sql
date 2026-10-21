-- Run once before enabling cluster healthchecks. Set meta_endpoint for each
-- monitored cluster to the exact meta.service_endpoint of its regional service.
-- NULL/unassigned clusters are not polled. This does not select an owner pod.
ALTER TABLE `ydb/MasterClusterExt.db` ADD COLUMN meta_endpoint Utf8;
