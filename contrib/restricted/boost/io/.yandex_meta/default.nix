self: super: with self; {
  boost_io = stdenv.mkDerivation rec {
    pname = "boost_io";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "io";
      rev = "boost-${version}";
      hash = "sha256-7O2qCHTybyhzqkJUftJ/7W06EhAOzKbX5ulYh5d0Es8=";
    };
  };
}
