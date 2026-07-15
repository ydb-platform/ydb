self: super: with self; {
  boost_program_options = stdenv.mkDerivation rec {
    pname = "boost_program_options";
    version = "1.91.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "program_options";
      rev = "boost-${version}";
      hash = "sha256-dcMub5JmS3Re/z9GoucWjVqtSSLSUaboPvM+EhPEfRI=";
    };
  };
}
