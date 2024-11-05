#!/usr/bin/env bash
set -eux
docker run --network=host --detach --name=postgres -e POSTGRES_PASSWORD=1234 -e POSTGRES_USER=root -e POSTGRES_DB=local --rm postgres:16
sleep 5
docker exec -i postgres psql postgres://root:1234@localhost:5432/local < columns.sql > columns.txt
docker exec -i postgres psql postgres://root:1234@localhost:5432/local < pg_class.sql > pg_class.txt
docker rm -f postgres

