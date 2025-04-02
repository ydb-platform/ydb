pragma ydb.optimizerhints =
'
    JoinOrder((cd (c ca)) ss)
    JoinType(c ca broadcast)
    JoinType(c ca cd broadcast)
';

select
    *
 from
  `/Root/test/ds/customer` c
  inner join `/Root/test/ds/customer_address` ca on c.c_current_addr_sk = ca.ca_address_sk
  inner join `/Root/test/ds/customer_demographics` cd on cd.cd_demo_sk = c.c_current_cdemo_sk
  left semi join `/Root/test/ds/store_sales` ss on c.c_customer_sk = ss.ss_customer_sk
 where
  ca_state in ('KY','GA','NM')
