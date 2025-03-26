self: super: with self; {
  boost_variant = stdenv.mkDerivation rec {
    pname = "boost_variant";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "variant";
      rev = "boost-${version}";
      hash = "sha256-vW5D4UKGNKlrfWPk8AEsTvrEr5vtuwegG83Z7eQ572w=";
    };
  };
}
