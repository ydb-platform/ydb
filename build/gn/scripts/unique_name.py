import sys
path = sys.argv[1]
unique_name = path.replace("/", "_").replace("-", "_")
print(unique_name)
