self: super: with self; {
  boost_config = stdenv.mkDerivation rec {
    pname = "boost_config";
    version = "1.92.0";

    src = fetchFromGitHub {
      owner = "boostorg";
      repo = "config";
      rev = "boost-${version}";
      hash = "sha256-KjLxYomgUt7TU/9yI5b+yxu6UH9Is0TLVdi5F22O4/A=";
    };
  };
}
