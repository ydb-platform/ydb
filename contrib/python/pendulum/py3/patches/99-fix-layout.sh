mv ./src/* .
rm -rf ./src
sed -E 's/(\s+)src\//\1/g' --in-place ya.make
