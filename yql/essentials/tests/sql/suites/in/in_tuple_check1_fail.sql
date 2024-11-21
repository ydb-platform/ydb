select AsTuple(1, 1) in (
  AsTuple(2, 1),
  AsTuple(1, 2),
  42 -- expect tuple, not data
)
