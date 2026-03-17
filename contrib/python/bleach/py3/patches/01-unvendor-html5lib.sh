find . -name '*.py' -exec sed -e 's/bleach._vendor.html5lib/html5lib/g' --in-place '{}' ';'
