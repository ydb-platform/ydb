self: super: with self; {
  boost_assert = stdenv.mkDerivation rec {
    pname = "boost_assert";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "assert";
      rev = "boost-${version}";
      hash = "sha256-sBQQ8efXevprZVGNOVnyiNGcYDL72BWBQe5KqHCQ8+U=";
    };
  };
}
