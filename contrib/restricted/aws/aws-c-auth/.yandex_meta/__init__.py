from devtools.yamaker.project import CMakeNinjaNixProject


aws_c_auth = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-auth",
    nixattr="aws-c-auth",
)
