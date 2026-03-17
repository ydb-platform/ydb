find . -name "*.py" -exec sed -i -e 's/from langchain\./from langchain_classic./g' {} \;
find . -name "*.py" -exec sed -i -e 's/from langchain_core\.memory import BaseMemory/from langchain_classic.base_memory import BaseMemory/g' {} \;
