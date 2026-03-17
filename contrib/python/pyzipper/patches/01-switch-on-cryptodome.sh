find . -name '*.py' -exec sed -e 's/from Cryptodome/from Crypto/g' --in-place '{}' ';'
find . -name '*.py' -exec sed -e 's/import Cryptodome/import Crypto/g' --in-place '{}' ';'
