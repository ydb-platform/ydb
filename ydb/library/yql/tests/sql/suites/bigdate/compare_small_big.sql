pragma warning("disable","4510");
select
  Yql::Date(AsAtom("1")) = Yql::Date32(AsAtom("1")),
  Yql::Date(AsAtom("1")) = Yql::Datetime64(AsAtom("86400")),
  Yql::Date(AsAtom("1")) = Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Datetime(AsAtom("86400")) = Yql::Date32(AsAtom("1")),
  Yql::Datetime(AsAtom("86400")) = Yql::Datetime64(AsAtom("86400")),
  Yql::Datetime(AsAtom("86400")) = Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Timestamp(AsAtom("86400000000")) = Yql::Date32(AsAtom("1")),
  Yql::Timestamp(AsAtom("86400000000")) = Yql::Datetime64(AsAtom("86400")),
  Yql::Timestamp(AsAtom("86400000000")) = Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Interval(AsAtom("1")) = Yql::Interval64(AsAtom("1")),

  Yql::Date(AsAtom("1")) < Yql::Date32(AsAtom("1")),
  Yql::Date(AsAtom("1")) < Yql::Datetime64(AsAtom("86400")),
  Yql::Date(AsAtom("1")) < Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Datetime(AsAtom("86400")) < Yql::Date32(AsAtom("1")),
  Yql::Datetime(AsAtom("86400")) < Yql::Datetime64(AsAtom("86400")),
  Yql::Datetime(AsAtom("86400")) < Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Timestamp(AsAtom("86400000000")) < Yql::Date32(AsAtom("1")),
  Yql::Timestamp(AsAtom("86400000000")) < Yql::Datetime64(AsAtom("86400")),
  Yql::Timestamp(AsAtom("86400000000")) < Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Interval(AsAtom("1")) < Yql::Interval64(AsAtom("1")),

  Yql::Date(AsAtom("1")) <= Yql::Date32(AsAtom("1")),
  Yql::Date(AsAtom("1")) <= Yql::Datetime64(AsAtom("86400")),
  Yql::Date(AsAtom("1")) <= Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Datetime(AsAtom("86400")) <= Yql::Date32(AsAtom("1")),
  Yql::Datetime(AsAtom("86400")) <= Yql::Datetime64(AsAtom("86400")),
  Yql::Datetime(AsAtom("86400")) <= Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Timestamp(AsAtom("86400000000")) <= Yql::Date32(AsAtom("1")),
  Yql::Timestamp(AsAtom("86400000000")) <= Yql::Datetime64(AsAtom("86400")),
  Yql::Timestamp(AsAtom("86400000000")) <= Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Interval(AsAtom("1")) <= Yql::Interval64(AsAtom("1")),

  Yql::Date(AsAtom("1")) > Yql::Date32(AsAtom("1")),
  Yql::Date(AsAtom("1")) > Yql::Datetime64(AsAtom("86400")),
  Yql::Date(AsAtom("1")) > Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Datetime(AsAtom("86400")) > Yql::Date32(AsAtom("1")),
  Yql::Datetime(AsAtom("86400")) > Yql::Datetime64(AsAtom("86400")),
  Yql::Datetime(AsAtom("86400")) > Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Timestamp(AsAtom("86400000000")) > Yql::Date32(AsAtom("1")),
  Yql::Timestamp(AsAtom("86400000000")) > Yql::Datetime64(AsAtom("86400")),
  Yql::Timestamp(AsAtom("86400000000")) > Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Interval(AsAtom("1")) > Yql::Interval64(AsAtom("1")),

  Yql::Date(AsAtom("1")) >= Yql::Date32(AsAtom("1")),
  Yql::Date(AsAtom("1")) >= Yql::Datetime64(AsAtom("86400")),
  Yql::Date(AsAtom("1")) >= Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Datetime(AsAtom("86400")) >= Yql::Date32(AsAtom("1")),
  Yql::Datetime(AsAtom("86400")) >= Yql::Datetime64(AsAtom("86400")),
  Yql::Datetime(AsAtom("86400")) >= Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Timestamp(AsAtom("86400000000")) >= Yql::Date32(AsAtom("1")),
  Yql::Timestamp(AsAtom("86400000000")) >= Yql::Datetime64(AsAtom("86400")),
  Yql::Timestamp(AsAtom("86400000000")) >= Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Interval(AsAtom("1")) >= Yql::Interval64(AsAtom("1")),

  Yql::Date(AsAtom("1")) != Yql::Date32(AsAtom("1")),
  Yql::Date(AsAtom("1")) != Yql::Datetime64(AsAtom("86400")),
  Yql::Date(AsAtom("1")) != Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Datetime(AsAtom("86400")) != Yql::Date32(AsAtom("1")),
  Yql::Datetime(AsAtom("86400")) != Yql::Datetime64(AsAtom("86400")),
  Yql::Datetime(AsAtom("86400")) != Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Timestamp(AsAtom("86400000000")) != Yql::Date32(AsAtom("1")),
  Yql::Timestamp(AsAtom("86400000000")) != Yql::Datetime64(AsAtom("86400")),
  Yql::Timestamp(AsAtom("86400000000")) != Yql::Timestamp64(AsAtom("86400000000")),

  Yql::Interval(AsAtom("1")) != Yql::Interval64(AsAtom("1"));
