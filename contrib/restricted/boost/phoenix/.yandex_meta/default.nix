self: super: with self; {
  boost_phoenix = stdenv.mkDerivation rec {
    pname = "boost_phoenix";
    version = "1.87.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "phoenix";
      rev = "boost-${version}";
      hash = "sha256-szO8RfKawPe1XhjRdWpHm9nii8dglpFQKlnQUGSwa4o=";
    };
  };
}
