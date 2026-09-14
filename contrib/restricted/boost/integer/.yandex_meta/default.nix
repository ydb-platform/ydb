self: super: with self; {
  boost_integer = stdenv.mkDerivation rec {
    pname = "boost_integer";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "integer";
      rev = "boost-${version}";
      hash = "sha256-DyxHLiuflTc9Gv+0gSFq/K+wBOCK0x/mBOWIjakuNT8=";
    };
  };
}
