self: super: with self; {
  boost_utility = stdenv.mkDerivation rec {
    pname = "boost_utility";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "utility";
      rev = "boost-${version}";
      hash = "sha256-MRy+4U+qfVYqvetB0NuqjwvGrv6DAnHondyxjXCS+7U=";
    };
  };
}
