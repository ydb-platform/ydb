A library for translating between absolute times (i.e., `TInstant`) and civil times (i.e.,
`NDatetime::TSimpleTM`) using the rules defined by a time zone (i.e., `NDatetime::TTimeZone`).

(the terms `absolute` and `civil` come from [cctz#fundamental-concepts][cctz-fundamental-concepts])

This is basically a wrapper around [CCTZ][cctz] with one important change: the time zone database is
in Arcadia and is compiled with the library (which means your executable will end up ~2MB larger).

See [contrib/libs/cctz/README][update] if you think zone database is outdated.

Quick start:
============
```
#include <library/cpp/timezone_conversion/convert.h>

// NDatetime::{GetLocalTimeZone(),GetUtcTimeZone()} are also available.
NDatetime::TTimeZone msk = NDatetime::GetTimeZone("Europe/Moscow");
TInstant now = TInstant::Now();
NDatetime::TSimpleTM civil = NDatetime::ToCivilTime(now, msk);
Cout << "Local time in Moscow is " << civil.ToString() << Endl;
TInstant absolute = NDatetime::ToAbsoluteTime(civil, msk);
Cout << "The current UNIX time is " << absolute.Seconds() << Endl;
```

[cctz-fundamental-concepts]: https://github.com/google/cctz#fundamental-concepts
[cctz]: https://github.com/google/cctz
[update]: https://a.yandex-team.ru/arc/trunk/arcadia/contrib/libs/cctz/tzdata/README?rev=2286180
