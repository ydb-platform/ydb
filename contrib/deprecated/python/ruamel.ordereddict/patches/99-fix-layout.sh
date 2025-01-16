mkdir -p ruamel/ordereddict/
mv __init__.py ruamel/ordereddict/
sed -e 's/__init__\.py/ruamel\/ordereddict\/__init__\.py/g' --in-place ya.make
