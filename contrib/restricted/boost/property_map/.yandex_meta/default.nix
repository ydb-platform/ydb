self: super: with self; {
  boost_property_map = stdenv.mkDerivation rec {
    pname = "boost_property_map";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "property_map";
      rev = "boost-${version}";
      hash = "sha256-cqXG3/FdNYU+Z5hP2trARydzGyBTn4zZLL6vyIdwj1g=";
    };
  };
}
