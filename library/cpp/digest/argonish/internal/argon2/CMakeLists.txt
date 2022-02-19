add_library(argonish-internal-argon2 INTERFACE)
target_link_libraries(argonish-internal-argon2 INTERFACE
  contrib-libs-cxxsupp
  yutil
  argonish-internal-blamka
  argonish-internal-blake2b
  cpp-threading-poor_man_openmp
)
