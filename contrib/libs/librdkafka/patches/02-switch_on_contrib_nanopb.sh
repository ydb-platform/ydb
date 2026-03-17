find . -type f | xargs -L1 sed -i 's|#include <nanopb/|#include <contrib/libs/nanopb/|g'
find . -type f | xargs -L1 sed -i 's|#include "nanopb/|#include "contrib/libs/nanopb/|g'
