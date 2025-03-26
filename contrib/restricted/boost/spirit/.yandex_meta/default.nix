self: super: with self; {
  boost_spirit = stdenv.mkDerivation rec {
    pname = "boost_spirit";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "spirit";
      rev = "boost-${version}";
      hash = "sha256-zWhInLUIlzFtwhvPlBnQ5l+B8StTcVnXk34KlItyYKg=";
    };
  };
}
