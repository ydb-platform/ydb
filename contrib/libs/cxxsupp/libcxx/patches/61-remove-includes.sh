sed -i 's/#  include <__pstl_algorithm>/#error #  include <__pstl_algorithm>/' include/algorithm
sed -i 's/#   include <__pstl_execution>/#error #   include <__pstl_execution>/' include/execution
sed -i 's/# include <__external_threading>/#error #include <__external_threading>/' include/__threading_support
sed -i 's/#   include <__pstl_memory>/#error #   include <__pstl_memory>/' include/memory
sed -i 's/#   include <__pstl_numeric>/#error #   include <__pstl_numeric>/' include/numeric
