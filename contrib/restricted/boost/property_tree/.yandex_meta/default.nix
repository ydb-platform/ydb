self: super: with self; {
  boost_property_tree = stdenv.mkDerivation rec {
    pname = "boost_property_tree";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "property_tree";
      rev = "boost-${version}";
      hash = "sha256-1OIOkO9Zild0rOEHN0XR3XuoIB7GiVc9AhMtWPfhMiI=";
    };
  };
}
