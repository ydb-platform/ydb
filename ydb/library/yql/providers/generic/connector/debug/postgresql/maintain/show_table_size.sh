#!/bin/bash

set -ex

export PGPASSWORD=qwerty12345

SCRIPT="
DROP TABLE IF EXISTS benchmark_1g;
CREATE TABLE benchmark_1g (id bigserial, col varchar(1024));
INSERT INTO benchmark_1g SELECT generate_series(1,1048576) AS id, REPEAT(md5(random()::text), 32) AS col;
"

sudo docker exec -it connector-postgresql psql -U crab -d dqrun -c "${SCRIPT}"

SCRIPT="
SELECT pg_size_pretty(pg_total_relation_size('public.benchmark_1g'));
"

sudo docker exec -it connector-postgresql psql -U crab -d dqrun -c "${SCRIPT}"
