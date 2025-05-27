# The script takes the content of your Protocol Buffer (.proto) file and processes it to generate a compressed and encoded message meta info. 
# This meta info needs to be passed to the config.
# 1. Run the script in your terminal.
# 2. Paste your .proto file content when prompted.
# 3. Press Ctrl+D to signal the end of input.
# 4. Receive the Base64-encoded, gzipped descriptor set as output and paste to "meta" field of your config.

ya make contrib/tools/protoc && tmp=$(mktemp --suffix=.proto) && cat > "$tmp" && ./contrib/tools/protoc/protoc -I "$(dirname "$tmp")" --include_imports --descriptor_set_out=/dev/stdout "$tmp" | gzip | base64 -w 0 && rm "$tmp"
