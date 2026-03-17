mv python/* .
rmdir python
sed -e 's/python\///g' --in-place ya.make
