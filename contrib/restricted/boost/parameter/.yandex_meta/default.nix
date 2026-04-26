self: super: with self; {
  boost_parameter = stdenv.mkDerivation rec {
    pname = "boost_parameter";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "parameter";
      rev = "boost-${version}";
      hash = "sha256-A8qoN392ZhZVj+rIFYai8ODY2r+ZKWKzC8mEcZzgH5k=";
    };
  };
}
