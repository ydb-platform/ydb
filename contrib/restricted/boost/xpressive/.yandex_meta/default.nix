self: super: with self; {
  boost_xpressive = stdenv.mkDerivation rec {
    pname = "boost_xpressive";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "xpressive";
      rev = "boost-${version}";
      hash = "sha256-SA7toFifjSPqmH5RGxl+o9rsTiqxNH4PaTLfKoIMRZ8=";
    };
  };
}
