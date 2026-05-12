self: super: with self; {
  boost_mpl = stdenv.mkDerivation rec {
    pname = "boost_mpl";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mpl";
      rev = "boost-${version}";
      hash = "sha256-93iIbMLvaV3VhlmeY7VyvGgVH0IiKPVyBIQGa2GqJec=";
    };
  };
}
