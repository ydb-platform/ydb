sed --in-place -r 's/^(.*)(\((WINAPI|NTAPI|CALLBACK).*)/PSUTIL_MAYBE_EXTERN \1\2/g' psutil/arch/windows/ntextapi.h
