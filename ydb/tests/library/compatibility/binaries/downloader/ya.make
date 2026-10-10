PY3_PROGRAM()
REQUIREMENTS(cpu:1)
    PEERDIR(
      contrib/python/boto3
      contrib/python/botocore
    )

    PY_SRCS(
       __main__.py
     )
END()