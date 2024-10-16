self: super: with self; rec {
  pname = "freetype";
  version = "2.13.3";

  # autoreconfHook doesn't work here somehow
  nativeBuildInputs = [ autoconf automake libtool ];

  buildInputs = [ gnumake zlib ];

  preConfigure = "./autogen.sh";

  CFLAGS = [
    "-DFT_CONFIG_OPTION_SYSTEM_ZLIB"
    "-DFT_DEBUG_LEVEL_TRACE"
    "-DFT_DEBUG_LOGGING"
  ];

  src = fetchFromGitLab {
    domain = "gitlab.freedesktop.org";
    owner = "freetype";
    repo = "freetype";
    rev = "VER-${self.lib.replaceStrings ["."] ["-"] version}";
    hash = "sha256-HJ4gKR+gLudollVsYhKQPKa6xAuM8RlCTwUhMQENFdQ=";
    leaveDotGit = true;
    fetchSubmodules = true;
  };

  patches = [];
}
