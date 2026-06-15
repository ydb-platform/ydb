self: super: with self; {
  boost_property_tree = stdenv.mkDerivation rec {
    pname = "boost_property_tree";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "property_tree";
      rev = "boost-${version}";
      hash = "sha256-aIX5KvM62kidfLOGW1DZQZhh4iYwNn3ZyOnUxhSgmVw=";
    };
  };
}
