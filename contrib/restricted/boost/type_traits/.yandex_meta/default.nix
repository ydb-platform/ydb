self: super: with self; {
  boost_type_traits = stdenv.mkDerivation rec {
    pname = "boost_type_traits";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "type_traits";
      rev = "boost-${version}";
      hash = "sha256-OH9x0V8WzASXYFwIPs3RAA4O1JD4dEm5wEjndpAM0T4=";
    };
  };
}
