#!/bin/bash -e

for file in $(find . -name '*.hpp'); do
	perl -0 -i -pe 's/\bcompat::/std::/g' "$file"
	perl -0 -i -pe 's/#include <boost\/compat\/.*>\n//gm' $file
done
