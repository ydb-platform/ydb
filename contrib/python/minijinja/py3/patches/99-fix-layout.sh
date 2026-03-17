mv ./python/* .
rm -rf ./python
sed -E 's/(\s+)python\//\1/g' --in-place ya.make
