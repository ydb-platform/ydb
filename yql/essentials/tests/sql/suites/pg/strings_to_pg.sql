select ToPg("foo"),ToPg(Utf8("bar")),ToPg(Yson("<a=1>[]")),ToPg(Json('{"a":1}')),ToPg(JsonDocument("[1,2,3]")),
       ToPg(Uuid('12345678-9abc-def0-1234-567890123456')), 
       ToPg(TzDate("2001-02-03,Europe/Moscow")),
       ToPg(TzDatetime("2001-02-03T04:05:06,Europe/Moscow")),
       ToPg(TzTimestamp("2001-02-03T04:05:06.789012,Europe/Moscow")),
       ToPg(TzDate32("1901-02-03,Europe/Moscow")),
       ToPg(TzDatetime64("1901-02-03T04:05:06,Europe/Moscow")),
       ToPg(TzTimestamp64("1901-02-03T04:05:06.789012,Europe/Moscow"));



