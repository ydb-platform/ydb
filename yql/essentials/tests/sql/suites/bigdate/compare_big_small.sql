pragma warning("disable","4510");
select
  3, Yql::Date32(AsAtom("1")) = Yql::Date(AsAtom("1")),
  4, Yql::Datetime64(AsAtom("86400")) = Yql::Date(AsAtom("1")),
  5, Yql::Timestamp64(AsAtom("86400000000")) = Yql::Date(AsAtom("1")),

  7, Yql::Date32(AsAtom("1")) = Yql::Datetime(AsAtom("86400")),
  8, Yql::Datetime64(AsAtom("86400")) = Yql::Datetime(AsAtom("86400")),
  9, Yql::Timestamp64(AsAtom("86400000000")) = Yql::Datetime(AsAtom("86400")),

  11, Yql::Date32(AsAtom("1")) = Yql::Timestamp(AsAtom("86400000000")),
  12, Yql::Datetime64(AsAtom("86400")) = Yql::Timestamp(AsAtom("86400000000")),
  13, Yql::Timestamp64(AsAtom("86400000000")) = Yql::Timestamp(AsAtom("86400000000")),

  15, Yql::Interval64(AsAtom("1")) = Yql::Interval(AsAtom("1")),

  17, Yql::Date32(AsAtom("1")) < Yql::Date(AsAtom("1")),
  18, Yql::Datetime64(AsAtom("86400")) < Yql::Date(AsAtom("1")),
  19, Yql::Timestamp64(AsAtom("86400000000")) < Yql::Date(AsAtom("1")),

  21, Yql::Date32(AsAtom("1")) < Yql::Datetime(AsAtom("86400")),
  22, Yql::Datetime64(AsAtom("86400")) < Yql::Datetime(AsAtom("86400")),
  23, Yql::Timestamp64(AsAtom("86400000000")) < Yql::Datetime(AsAtom("86400")),

  25, Yql::Date32(AsAtom("1")) < Yql::Timestamp(AsAtom("86400000000")),
  26, Yql::Datetime64(AsAtom("86400")) < Yql::Timestamp(AsAtom("86400000000")),
  27, Yql::Timestamp64(AsAtom("86400000000")) < Yql::Timestamp(AsAtom("86400000000")),

  29, Yql::Interval64(AsAtom("1")) < Yql::Interval(AsAtom("1")),

  31, Yql::Date32(AsAtom("1")) <= Yql::Date(AsAtom("1")),
  32, Yql::Datetime64(AsAtom("86400")) <= Yql::Date(AsAtom("1")),
  33, Yql::Timestamp64(AsAtom("86400000000")) <= Yql::Date(AsAtom("1")),

  35, Yql::Date32(AsAtom("1")) <= Yql::Datetime(AsAtom("86400")),
  36, Yql::Datetime64(AsAtom("86400")) <= Yql::Datetime(AsAtom("86400")),
  37, Yql::Timestamp64(AsAtom("86400000000")) <= Yql::Datetime(AsAtom("86400")),

  39, Yql::Date32(AsAtom("1")) <= Yql::Timestamp(AsAtom("86400000000")),
  40, Yql::Datetime64(AsAtom("86400")) <= Yql::Timestamp(AsAtom("86400000000")),
  41, Yql::Timestamp64(AsAtom("86400000000")) <= Yql::Timestamp(AsAtom("86400000000")),

  43, Yql::Interval64(AsAtom("1")) <= Yql::Interval(AsAtom("1")),

  45, Yql::Date32(AsAtom("1")) > Yql::Date(AsAtom("1")),
  46, Yql::Datetime64(AsAtom("86400")) > Yql::Date(AsAtom("1")),
  47, Yql::Timestamp64(AsAtom("86400000000")) > Yql::Date(AsAtom("1")),

  49, Yql::Date32(AsAtom("1")) > Yql::Datetime(AsAtom("86400")),
  50, Yql::Datetime64(AsAtom("86400")) > Yql::Datetime(AsAtom("86400")),
  51, Yql::Timestamp64(AsAtom("86400000000")) > Yql::Datetime(AsAtom("86400")),

  53, Yql::Date32(AsAtom("1")) > Yql::Timestamp(AsAtom("86400000000")),
  54, Yql::Datetime64(AsAtom("86400")) > Yql::Timestamp(AsAtom("86400000000")),
  55, Yql::Timestamp64(AsAtom("86400000000")) > Yql::Timestamp(AsAtom("86400000000")),

  57, Yql::Interval64(AsAtom("1")) > Yql::Interval(AsAtom("1")),

  59, Yql::Date32(AsAtom("1")) >= Yql::Date(AsAtom("1")),
  60, Yql::Datetime64(AsAtom("86400")) >= Yql::Date(AsAtom("1")),
  61, Yql::Timestamp64(AsAtom("86400000000")) >= Yql::Date(AsAtom("1")),

  63, Yql::Date32(AsAtom("1")) >= Yql::Datetime(AsAtom("86400")),
  64, Yql::Datetime64(AsAtom("86400")) >= Yql::Datetime(AsAtom("86400")),
  65, Yql::Timestamp64(AsAtom("86400000000")) >= Yql::Datetime(AsAtom("86400")),

  67, Yql::Date32(AsAtom("1")) >= Yql::Timestamp(AsAtom("86400000000")),
  68, Yql::Datetime64(AsAtom("86400")) >= Yql::Timestamp(AsAtom("86400000000")),
  69, Yql::Timestamp64(AsAtom("86400000000")) >= Yql::Timestamp(AsAtom("86400000000")),

  71, Yql::Interval64(AsAtom("1")) >= Yql::Interval(AsAtom("1")),

  73, Yql::Date32(AsAtom("1")) != Yql::Date(AsAtom("1")),
  74, Yql::Datetime64(AsAtom("86400")) != Yql::Date(AsAtom("1")),
  75, Yql::Timestamp64(AsAtom("86400000000")) != Yql::Date(AsAtom("1")),

  77, Yql::Date32(AsAtom("1")) != Yql::Datetime(AsAtom("86400")),
  78, Yql::Datetime64(AsAtom("86400")) != Yql::Datetime(AsAtom("86400")),
  79, Yql::Timestamp64(AsAtom("86400000000")) != Yql::Datetime(AsAtom("86400")),

  81, Yql::Date32(AsAtom("1")) != Yql::Timestamp(AsAtom("86400000000")),
  82, Yql::Datetime64(AsAtom("86400")) != Yql::Timestamp(AsAtom("86400000000")),
  83, Yql::Timestamp64(AsAtom("86400000000")) != Yql::Timestamp(AsAtom("86400000000")),

  85, Yql::Interval64(AsAtom("1")) != Yql::Interval(AsAtom("1"));
