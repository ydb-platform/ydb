self: super: with self; {
  boost_property_map = stdenv.mkDerivation rec {
    pname = "boost_property_map";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "property_map";
      rev = "boost-${version}";
      hash = "sha256-sdI/2biFn2yVAE8h0REyUYnMttfllLcQc7OUBPqk5Qk=";
    };
  };
}
