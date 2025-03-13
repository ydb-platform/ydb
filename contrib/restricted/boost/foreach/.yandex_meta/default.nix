self: super: with self; {
  boost_foreach = stdenv.mkDerivation rec {
    pname = "boost_foreach";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "foreach";
      rev = "boost-${version}";
      hash = "sha256-7Pt5487/5sFJmhKY4lz80Y6Ag/8yd0jq2CSbVoGijWI=";
    };
  };
}
