self: super: with self; {
  boost_property_map = stdenv.mkDerivation rec {
    pname = "boost_property_map";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "property_map";
      rev = "boost-${version}";
      hash = "sha256-UYhn7r2afQAa/0g3qi36pNwIibg2+k6IrYP9YbPlo98=";
    };
  };
}
