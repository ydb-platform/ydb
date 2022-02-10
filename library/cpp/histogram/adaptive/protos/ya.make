PROTO_LIBRARY()

OWNER(g:crawl)

SRCS(
    histo.proto
)

IF (NOT PY_PROTOS_FOR) 
    EXCLUDE_TAGS(GO_PROTO) 
ENDIF() 
 
END()
