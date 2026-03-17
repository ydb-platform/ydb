mv sentencepiece/src/*.proto sentencepiece
rmdir sentencepiece/src
sed -e 's|sentencepiece/src/|sentencepiece/|g' -i ya.make
