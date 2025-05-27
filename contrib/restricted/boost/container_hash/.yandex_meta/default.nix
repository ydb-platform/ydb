self: super: with self; {
  boost_container_hash = stdenv.mkDerivation rec {
    pname = "boost_container_hash";
    version = "1.88.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "container_hash";
      rev = "boost-${version}";
      hash = "sha256-0LilNldSf2i+QeO9W1qiMQI5QCa6tCmAyLv2feKL3W4=";
    };
  };
}
