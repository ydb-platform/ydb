SUBSCRIBER(g:yql)

IF (NOT EXPORT_CMAKE)
    RECURSE(
        ast
        build
        core
        docs
        minikql
        parser
        protos
        providers
        public
        sql
        tests
        tools
        types
        udfs
        utils
    )
ENDIF()
