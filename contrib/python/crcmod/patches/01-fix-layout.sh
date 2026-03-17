mv python3/src .
mv python3/crcmod .
rmdir python3
sed -e 's|python3/||g' --in-place ya.make
