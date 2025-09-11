self: super: with self; {
  boost_mp11 = stdenv.mkDerivation rec {
    pname = "boost_mp11";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mp11";
      rev = "boost-${version}";
      hash = "sha256-HcQJ/PXBQdWVjGZy28X2LxVRfjV2nkeLTusNjT9ssXI=";
    };
  };
}
