add_library(lwtrace-mon-analytics)
target_link_libraries(lwtrace-mon-analytics PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(lwtrace-mon-analytics PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/mon/analytics/analytics.cpp
)
