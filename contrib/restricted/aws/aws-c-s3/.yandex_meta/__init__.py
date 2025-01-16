from devtools.yamaker.project import CMakeNinjaNixProject


aws_c_s3 = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-s3",
    nixattr="aws-c-s3",
)
