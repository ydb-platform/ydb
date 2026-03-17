mv pysrc/* .
rmdir pysrc
sed -e 's/pysrc\///g' --in-place ya.make
