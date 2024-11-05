tables="catalog_page
catalog_returns
catalog_sales
customer
customer_address
customer_demographics
date_dim
inventory
item
store_returns
store_sales
time_dim
web_returns
web_sales"

for t in $tables
do
	download $t 60
done

tables="call_center
household_demographics
income_band
promotion
reason
ship_mode
store
warehouse
web_page
web_site"
for t in $tables
do
	download $t 1
done

