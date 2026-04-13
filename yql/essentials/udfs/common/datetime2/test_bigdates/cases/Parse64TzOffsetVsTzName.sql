$parse = Datetime::Parse64("%Y-%m-%dT%H:%M:%S%z at %Z");

select $parse("1925-12-15T12:45:54+03:00 at Europe/Moscow");
