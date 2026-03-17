set -xue

echo '#pragma clang system_header' > _
cat Eigen/src/Core/Reverse.h >> _
mv _ Eigen/src/Core/Reverse.h
