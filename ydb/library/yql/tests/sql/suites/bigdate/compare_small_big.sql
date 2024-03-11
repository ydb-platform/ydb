pragma warning("disable","4510");
select
  3, Yql::Date(AsAtom("1")) = Yql::Date32(AsAtom("1")),
  4, Yql::Date(AsAtom("1")) = Yql::Datetime64(AsAtom("86400")),
  5, Yql::Date(AsAtom("1")) = Yql::Timestamp64(AsAtom("86400000000")),

  7, Yql::Datetime(AsAtom("86400")) = Yql::Date32(AsAtom("1")),
  8, Yql::Datetime(AsAtom("86400")) = Yql::Datetime64(AsAtom("86400")),
  9, Yql::Datetime(AsAtom("86400")) = Yql::Timestamp64(AsAtom("86400000000")),

  11, Yql::Timestamp(AsAtom("86400000000")) = Yql::Date32(AsAtom("1")),
  12, Yql::Timestamp(AsAtom("86400000000")) = Yql::Datetime64(AsAtom("86400")),
  13, Yql::Timestamp(AsAtom("86400000000")) = Yql::Timestamp64(AsAtom("86400000000")),

  15, Yql::Interval(AsAtom("1")) = Yql::Interval64(AsAtom("1")),

  17, Yql::Date(AsAtom("1")) < Yql::Date32(AsAtom("1")),
  18, Yql::Date(AsAtom("1")) < Yql::Datetime64(AsAtom("86400")),
  19, Yql::Date(AsAtom("1")) < Yql::Timestamp64(AsAtom("86400000000")),

  21, Yql::Datetime(AsAtom("86400")) < Yql::Date32(AsAtom("1")),
  22, Yql::Datetime(AsAtom("86400")) < Yql::Datetime64(AsAtom("86400")),
  23, Yql::Datetime(AsAtom("86400")) < Yql::Timestamp64(AsAtom("86400000000")),

  25, Yql::Timestamp(AsAtom("86400000000")) < Yql::Date32(AsAtom("1")),
  26, Yql::Timestamp(AsAtom("86400000000")) < Yql::Datetime64(AsAtom("86400")),
  27, Yql::Timestamp(AsAtom("86400000000")) < Yql::Timestamp64(AsAtom("86400000000")),

  29, Yql::Interval(AsAtom("1")) < Yql::Interval64(AsAtom("1")),

  31, Yql::Date(AsAtom("1")) <= Yql::Date32(AsAtom("1")),
  32, Yql::Date(AsAtom("1")) <= Yql::Datetime64(AsAtom("86400")),
  33, Yql::Date(AsAtom("1")) <= Yql::Timestamp64(AsAtom("86400000000")),

  35, Yql::Datetime(AsAtom("86400")) <= Yql::Date32(AsAtom("1")),
  36, Yql::Datetime(AsAtom("86400")) <= Yql::Datetime64(AsAtom("86400")),
  37, Yql::Datetime(AsAtom("86400")) <= Yql::Timestamp64(AsAtom("86400000000")),

  39, Yql::Timestamp(AsAtom("86400000000")) <= Yql::Date32(AsAtom("1")),
  40, Yql::Timestamp(AsAtom("86400000000")) <= Yql::Datetime64(AsAtom("86400")),
  41, Yql::Timestamp(AsAtom("86400000000")) <= Yql::Timestamp64(AsAtom("86400000000")),

  43, Yql::Interval(AsAtom("1")) <= Yql::Interval64(AsAtom("1")),

  45, Yql::Date(AsAtom("1")) > Yql::Date32(AsAtom("1")),
  46, Yql::Date(AsAtom("1")) > Yql::Datetime64(AsAtom("86400")),
  47, Yql::Date(AsAtom("1")) > Yql::Timestamp64(AsAtom("86400000000")),

  49, Yql::Datetime(AsAtom("86400")) > Yql::Date32(AsAtom("1")),
  50, Yql::Datetime(AsAtom("86400")) > Yql::Datetime64(AsAtom("86400")),
  51, Yql::Datetime(AsAtom("86400")) > Yql::Timestamp64(AsAtom("86400000000")),

  53, Yql::Timestamp(AsAtom("86400000000")) > Yql::Date32(AsAtom("1")),
  54, Yql::Timestamp(AsAtom("86400000000")) > Yql::Datetime64(AsAtom("86400")),
  55, Yql::Timestamp(AsAtom("86400000000")) > Yql::Timestamp64(AsAtom("86400000000")),

  57, Yql::Interval(AsAtom("1")) > Yql::Interval64(AsAtom("1")),

  59, Yql::Date(AsAtom("1")) >= Yql::Date32(AsAtom("1")),
  60, Yql::Date(AsAtom("1")) >= Yql::Datetime64(AsAtom("86400")),
  61, Yql::Date(AsAtom("1")) >= Yql::Timestamp64(AsAtom("86400000000")),

  63, Yql::Datetime(AsAtom("86400")) >= Yql::Date32(AsAtom("1")),
  64, Yql::Datetime(AsAtom("86400")) >= Yql::Datetime64(AsAtom("86400")),
  65, Yql::Datetime(AsAtom("86400")) >= Yql::Timestamp64(AsAtom("86400000000")),

  67, Yql::Timestamp(AsAtom("86400000000")) >= Yql::Date32(AsAtom("1")),
  68, Yql::Timestamp(AsAtom("86400000000")) >= Yql::Datetime64(AsAtom("86400")),
  69, Yql::Timestamp(AsAtom("86400000000")) >= Yql::Timestamp64(AsAtom("86400000000")),

  71, Yql::Interval(AsAtom("1")) >= Yql::Interval64(AsAtom("1")),

  73, Yql::Date(AsAtom("1")) != Yql::Date32(AsAtom("1")),
  74, Yql::Date(AsAtom("1")) != Yql::Datetime64(AsAtom("86400")),
  75, Yql::Date(AsAtom("1")) != Yql::Timestamp64(AsAtom("86400000000")),

  77, Yql::Datetime(AsAtom("86400")) != Yql::Date32(AsAtom("1")),
  78, Yql::Datetime(AsAtom("86400")) != Yql::Datetime64(AsAtom("86400")),
  79, Yql::Datetime(AsAtom("86400")) != Yql::Timestamp64(AsAtom("86400000000")),

  81, Yql::Timestamp(AsAtom("86400000000")) != Yql::Date32(AsAtom("1")),
  82, Yql::Timestamp(AsAtom("86400000000")) != Yql::Datetime64(AsAtom("86400")),
  83, Yql::Timestamp(AsAtom("86400000000")) != Yql::Timestamp64(AsAtom("86400000000")),

  85, Yql::Interval(AsAtom("1")) != Yql::Interval64(AsAtom("1"));
