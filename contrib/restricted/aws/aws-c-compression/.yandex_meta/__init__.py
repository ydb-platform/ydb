from devtools.yamaker.project import CMakeNinjaNixProject


aws_c_compression = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-compression",
    nixattr="aws-c-compression",
)
