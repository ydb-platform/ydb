self: super: with self; {
  boost_thread = stdenv.mkDerivation rec {
    pname = "boost_thread";
    version = "1.89.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "thread";
      rev = "boost-${version}";
      hash = "sha256-vkLjfL6x4jwnBDIpcRA9ZOIz9058Tx0Z7u7kw67wihY=";
    };
  };
}
