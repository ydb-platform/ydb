self: super: with self; {
  boost_property_tree = stdenv.mkDerivation rec {
    pname = "boost_property_tree";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "property_tree";
      rev = "boost-${version}";
      hash = "sha256-hV0ip/n/OMnR6iLKKLmKuQlko/Oi8pZTkOQ2OyXjT5E=";
    };
  };
}
