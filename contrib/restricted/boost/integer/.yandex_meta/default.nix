self: super: with self; {
  boost_integer = stdenv.mkDerivation rec {
    pname = "boost_integer";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "integer";
      rev = "boost-${version}";
      hash = "sha256-1ahgl0abaaFM3+itvtAMuyWQ0IHrViQfXHG0dGrXstw=";
    };
  };
}
