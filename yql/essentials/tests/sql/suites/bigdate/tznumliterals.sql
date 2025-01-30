pragma warning("disable","4510");

select 
cast(Yql::Date32(AsAtom('-53375809')) as string),
Yql::TzDate32(AsAtom('-53375809,UTC')),
Yql::TzDate32(AsAtom('-53375809,Europe/Moscow')),
Yql::TzDate32(AsAtom('-53375809,America/Los_Angeles')),
cast(Yql::Date32(AsAtom('53375807')) as string),
Yql::TzDate32(AsAtom('53375807,UTC')),
Yql::TzDate32(AsAtom('53375807,Europe/Moscow')),
Yql::TzDate32(AsAtom('53375807,America/Los_Angeles')),
cast(Yql::Date32(AsAtom('-719162')) as string),
Yql::TzDate32(AsAtom('-719162,UTC')),
Yql::TzDate32(AsAtom('-719162,Europe/Moscow')),
cast(Yql::Date32(AsAtom('-719163')) as string),
Yql::TzDate32(AsAtom('-719163,UTC')),
Yql::TzDate32(AsAtom('-719163,Europe/Moscow'));

select 
cast(Yql::Datetime64(AsAtom('-4611669897600')) as string),
Yql::TzDatetime64(AsAtom('-4611669897600,UTC')),
Yql::TzDatetime64(AsAtom('-4611669897600,Europe/Moscow')),
Yql::TzDatetime64(AsAtom('-4611669897600,America/Los_Angeles')),
cast(Yql::Datetime64(AsAtom('4611669811199')) as string),
Yql::TzDatetime64(AsAtom('4611669811199,UTC')),
Yql::TzDatetime64(AsAtom('4611669811199,Europe/Moscow')),
Yql::TzDatetime64(AsAtom('4611669811199,America/Los_Angeles')),
cast(Yql::Datetime64(AsAtom('-62135596800')) as string),
Yql::TzDatetime64(AsAtom('-62135596800,UTC')),
Yql::TzDatetime64(AsAtom('-62135596800,Europe/Moscow')),
cast(Yql::Datetime64(AsAtom('-62135596801')) as string),
Yql::TzDatetime64(AsAtom('-62135596801,UTC')),
Yql::TzDatetime64(AsAtom('-62135596801,Europe/Moscow'));


select 
cast(Yql::Timestamp64(AsAtom('-4611669897600000000')) as string),
Yql::TzTimestamp64(AsAtom('-4611669897600000000,UTC')),
Yql::TzTimestamp64(AsAtom('-4611669897600000000,Europe/Moscow')),
Yql::TzTimestamp64(AsAtom('-4611669897600000000,America/Los_Angeles')),
cast(Yql::Timestamp64(AsAtom('4611669811199999999')) as string),
Yql::TzTimestamp64(AsAtom('4611669811199999999,UTC')),
Yql::TzTimestamp64(AsAtom('4611669811199999999,Europe/Moscow')),
Yql::TzTimestamp64(AsAtom('4611669811199999999,America/Los_Angeles')),
cast(Yql::Timestamp64(AsAtom('-62135596800000000')) as string),
Yql::TzTimestamp64(AsAtom('-62135596800000000,UTC')),
Yql::TzTimestamp64(AsAtom('-62135596800000000,Europe/Moscow')),
cast(Yql::Timestamp64(AsAtom('-62135596800000001')) as string),
Yql::TzTimestamp64(AsAtom('-62135596800000001,UTC')),
Yql::TzTimestamp64(AsAtom('-62135596800000001,Europe/Moscow'));

