
tables="customer lineitem orders part partsupp supplier"
for t in $tables
do
	download $t 30
done

tables="region nation"
for t in $tables
do
	download $t 1
done

