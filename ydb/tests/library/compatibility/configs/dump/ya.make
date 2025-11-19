RECURSE(dumper)

UNION()

IF(GIT_BRANCH)
    SET(BRANCH_PARAM --override-branch="${GIT_BRANCH}")
ENDIF()

IF(GIT_COMMIT_SHA)
    SET(COMMIT_PARAM --override-commit="${GIT_COMMIT_SHA}")
ENDIF()

RUN_PROGRAM(
    ydb/tests/library/compatibility/configs/dump/dumper ${BRANCH_PARAM} ${COMMIT_PARAM}
    STDOUT_NOAUTO config-meta.json
)

END()
