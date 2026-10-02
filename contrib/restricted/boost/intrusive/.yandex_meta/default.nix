self: super: with self; {
  boost_intrusive = stdenv.mkDerivation rec {
    pname = "boost_intrusive";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "intrusive";
      rev = "boost-${version}";
      hash = "sha256-jCXR4QywnvhJd/kNcup5AtoO3HEzD2hsyEt4RGDdvyc=";
    };
  };
}
