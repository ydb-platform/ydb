self: super: with self; {
  boost_detail = stdenv.mkDerivation rec {
    pname = "boost_detail";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "detail";
      rev = "boost-${version}";
      hash = "sha256-jHO82OPWXnk13VQh7rGFf+OCBeZc7g7RxCMoRTCunoM=";
    };
  };
}
