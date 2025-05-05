self: super: with self; {
  boost_filesystem = stdenv.mkDerivation rec {
    pname = "boost_filesystem";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "filesystem";
      rev = "boost-${version}";
      hash = "sha256-D2UOc/SuQ1wI7D82XxaSCaGgivb0Rgyy4i5/KmSpdRA=";
    };
  };
}
