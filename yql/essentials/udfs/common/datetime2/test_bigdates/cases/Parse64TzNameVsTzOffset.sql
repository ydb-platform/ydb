$parse = Datetime::Parse64("%Y-%m-%dT%H:%M:%S,%Z with %z offset");

select $parse("1925-12-15T12:45:54,Europe/Moscow with +03:00 offset");
