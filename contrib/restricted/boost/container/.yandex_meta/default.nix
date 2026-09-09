self: super: with self; {
  boost_container = stdenv.mkDerivation rec {
    pname = "boost_container";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container";
      rev = "boost-${version}";
      hash = "sha256-TVVJ7zxoo0+ExK8nL+ecUr0IJGQB68RYd/RVfxeMfSg=";
    };
  };
}
