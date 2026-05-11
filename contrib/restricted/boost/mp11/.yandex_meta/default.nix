self: super: with self; {
  boost_mp11 = stdenv.mkDerivation rec {
    pname = "boost_mp11";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "mp11";
      rev = "boost-${version}";
      hash = "sha256-wyRzA5EVtUqKyuOuqz8EXinXy0Vp1Q8x95qWCcL36kA=";
    };
  };
}
