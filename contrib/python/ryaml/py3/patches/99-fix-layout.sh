mv ./py-src/* .
rm -rf ./py-src
sed -E 's/(\s+)py-src\//\1/g' --in-place ya.make
