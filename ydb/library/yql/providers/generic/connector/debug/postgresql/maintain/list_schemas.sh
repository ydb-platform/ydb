#!/bin/bash

set -ex

export PGPASSWORD=qwerty12345

SCRIPT="
select schema_name
from information_schema.schemata;
"

sudo docker exec -it connector-postgresql psql -U crab -d dqrun -c "${SCRIPT}"


SCRIPT="
select nspname
from pg_catalog.pg_namespace;
"

sudo docker exec -it connector-postgresql psql -U crab -d dqrun -c "${SCRIPT}"
