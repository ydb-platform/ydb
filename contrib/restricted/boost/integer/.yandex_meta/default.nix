self: super: with self; {
  boost_integer = stdenv.mkDerivation rec {
    pname = "boost_integer";
    version = "1.90.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "integer";
      rev = "boost-${version}";
      hash = "sha256-kluVaYMoVwqdHiL6G8sHXgH9M5sWU6FEFGuBzfzBqkM=";
    };
  };
}
