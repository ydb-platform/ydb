mv python/* .
rmdir python
sed -E 's/(\s+)python\//\1/g' --in-place ya.make
#sed -E "s/'python' \/ 'pydantic_core'/'pydantic_core'/" --in-place generate_self_schema.py
