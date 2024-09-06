RECURSE(
    common # Keep this library pure of dependencies on YDB protofiles
    service # Use YDB protofiles only here
)
