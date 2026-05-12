$parse = Datetime::Parse("%Y-%m-%dT%H:%M:%S%z at %Z");

select $parse("2025-12-15T12:45:54+03:00 at Europe/Moscow");
