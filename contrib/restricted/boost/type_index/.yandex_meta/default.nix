self: super: with self; {
  boost_type_index = stdenv.mkDerivation rec {
    pname = "boost_type_index";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_index";
      rev = "boost-${version}";
      hash = "sha256-4EJhzd6aTLhk1TzwG6TgBYNF/eFvvOO//eWh2+kgZnk=";
    };
  };
}
