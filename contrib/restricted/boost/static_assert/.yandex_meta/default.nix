self: super: with self; {
  boost_static_assert = stdenv.mkDerivation rec {
    pname = "boost_static_assert";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "static_assert";
      rev = "boost-${version}";
      hash = "sha256-eqL5ociucdx7tAxdN2rNY4JtX/vI0e8mOPB50VnLzoA=";
    };
  };
}
