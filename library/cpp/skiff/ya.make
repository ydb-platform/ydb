LIBRARY()

SRCS(
  skiff.cpp
  skiff_schema.cpp
  skiff_validator.cpp
  zerocopy_output_writer.cpp
)

GENERATE_ENUM_SERIALIZATION(public.h)

END()

RECURSE_FOR_TESTS(
  unittests
)
