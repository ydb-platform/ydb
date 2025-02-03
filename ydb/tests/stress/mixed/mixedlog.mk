clean.dst:
	$(YDB) -e $(ENDPOINT) -d $(TENANT) workload log clean

init.dst:
	$(YDB) -e $(ENDPOINT) -d $(TENANT) workload log init --int-cols 5 --str-cols 5

bulk_upsert.dst:
	$(YDB) -e $(ENDPOINT) -d $(TENANT) workload log run bulk_upsert -s $(DURATION) -t $(BULK_UPSERT_THREADS) --rows $(BULK_UPSERT_ROWS) --window $(WINDOW) --len 1000 --int-cols 5 --str-cols 5 2>&1 | tee upsert_out

select.dst:
	sleep $(SELECT_DELAY); echo Started selects; $(YDB) -e $(ENDPOINT) -d $(TENANT) workload log run select -s $(SELECT_DURATION) -t $(SELECT_THREADS) --rows $(SELECT_ROWS) >select_out 2>select_err

update.dst:
	sleep $(UPSERT_DELAY); echo Started upserts; $(YDB) -e $(ENDPOINT) -d $(TENANT) workload log run upsert -s $(UPSERT_DURATION) -t $(UPSERT_THREADS) --rows $(UPSERT_ROWS) --len 1000  --int-cols 5 --str-cols 5 > update_out 2>update_err

ifndef NO_INIT
bulk_upsert.dst: init.dst
select.dst: init.dst
update.dst: init.dst
endif

all.dst: bulk_upsert.dst select.dst update.dst
	$(YDB) -e $(ENDPOINT) -d $(TENANT) workload log clean
