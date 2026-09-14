PY3_PROGRAM()
    PEERDIR(
      contrib/python/boto3
      contrib/python/botocore
    )

    PY_SRCS(
       __main__.py
     )
END()