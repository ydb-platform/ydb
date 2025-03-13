self: super: with self; {
  boost_static_assert = stdenv.mkDerivation rec {
    pname = "boost_static_assert";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "static_assert";
      rev = "boost-${version}";
      hash = "sha256-r2gxsp0b1HvPu4uvr6CBqjMrqRmYWbakv5QDCNQX6rc=";
    };
  };
}
