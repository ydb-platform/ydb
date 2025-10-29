self: super: with self; {
  boost_integer = stdenv.mkDerivation rec {
    pname = "boost_integer";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "integer";
      rev = "boost-${version}";
      hash = "sha256-lr90bVhBJIWjYAVTPLrqZI8NpQQnRbWTmJ9sS+yJvbI=";
    };
  };
}
