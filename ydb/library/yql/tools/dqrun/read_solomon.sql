select s.*, cast(c_timestamp as string) from solomon_prod.my_service
with (
  schema = (
    a_kind string not null,
    b_type string not null,
    c_timestamp Datetime not null,
    d_value double not null,
    e_labels dict<string, string> not null
  )
) as s
limit 10;