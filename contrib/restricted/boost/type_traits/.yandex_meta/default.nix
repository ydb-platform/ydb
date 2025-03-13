self: super: with self; {
  boost_type_traits = stdenv.mkDerivation rec {
    pname = "boost_type_traits";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_traits";
      rev = "boost-${version}";
      hash = "sha256-kunJMhryv/tq+bsX4gNVYFkkQfF5MJtIzw/sCgdbyqw=";
    };
  };
}
