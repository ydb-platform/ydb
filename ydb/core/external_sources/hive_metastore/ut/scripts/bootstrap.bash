set -e

for i in $(seq 0 80); do
    if trino --execute 'select 1' http://trino:8080; then
        break
    fi
    sleep 1
done

trino --file /scripts/create_table.sql http://trino:8080
