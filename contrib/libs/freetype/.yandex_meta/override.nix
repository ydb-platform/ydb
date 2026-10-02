self: super: with self; rec {
  pname = "freetype";
  version = "2.13.3";

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

  # autoreconfHook doesn't work here somehow
  nativeBuildInputs = [ autoconf automake libtool ];

  buildInputs = [ gnumake zlib ];

  preConfigure = "./autogen.sh";

  configureFlags = [
    "--build=x86_64-unknown-linux-gnu"
  ];

  CFLAGS = [
    "-DFT_CONFIG_OPTION_SYSTEM_ZLIB"
    "-DFT_DEBUG_LEVEL_TRACE"
    "-DFT_DEBUG_LOGGING"
  ];
}
