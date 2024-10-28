from devtools.yamaker.project import CMakeNinjaNixProject


aws_c_sdkutils = CMakeNinjaNixProject(
    arcdir="contrib/restricted/aws/aws-c-sdkutils",
    nixattr="aws-c-sdkutils",
    ignore_targets=["aws-c-sdkutils-tests"],
)
