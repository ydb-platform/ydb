self: super: with self; let

  optional-lite = stdenv.mkDerivation rec {
    name = "optional-lite";
    version = "3.6.0";

    src = fetchFromGitHub {
      owner = "martinmoene";
      repo = "optional-lite";
      rev = "v${version}";
      sha256 = "sha256-qmKuxYc0cpoOtRRb4okJZ8pYPvzQid1iqBctnhGlz6M=";
    };

    nativeBuildInputs = [ cmake ];

    cmakeFlags = [
      "-DOPTIONAL_LITE_OPT_BUILD_TESTS=OFF"
    ];
  };

  variant-lite = stdenv.mkDerivation rec {
    pname = "variant-lite";
    version = "2.0.0";

    src = fetchFromGitHub {
      owner = "martinmoene";
      repo = "variant-lite";
      rev = "v${version}";
      hash = "sha256-zLyzNzeD0C4e7CYqCCsPzkqa2cH5pSbL9vNVIxdkEfc=";
    };

    nativeBuildInputs = [ cmake ];

    cmakeFlags = [
      "-DVARIANT_LITE_OPT_BUILD_TESTS=OFF"
    ];
  };

  string-view-lite = stdenv.mkDerivation rec {
    pname = "string-view-lite";
    version = "1.7.0";

    src = fetchFromGitHub {
      owner = "martinmoene";
      repo = "string-view-lite";
      rev = "v${version}";
      hash = "sha256-L7b0MGp9grE/a9ppzsPe6UzEoA/yK3y1VD24OB7oadw=";
    };

    nativeBuildInputs = [ cmake ];

    cmakeFlags = [
      "-DSTRING_VIEW_LITE_OPT_BUILD_TESTS=OFF"
    ];
  };

in {
  jinja2cpp = stdenv.mkDerivation rec {
    pname = "jinja2cpp";
    version = "1.3.1";

    src = fetchFromGitHub {
      owner = "jinja2cpp";
      repo = "Jinja2Cpp";
      rev = "${version}";
      hash = "sha256-pWNHugUab0ACuhOivw4LZFcHhwxDSMsVz8hDiw/XVW8=";
    };

    patches = [ ./externals.patch ./boost-external.patch ];

    nativeBuildInputs = [ cmake ];

    cmakeFlags = [
      "-DJINJA2CPP_DEPS_MODE=external"
      "-DJINJA2CPP_CXX_STANDARD=17"
      "-DJINJA2CPP_BUILD_TESTS=FALSE"

      # FIXME: this setting has no effect,
      # as CMakeLists.txt in upstream unconditionally overwrites it.
      # We will emulate necessary behavior during post_install()
      "-DJINJA2CPP_WITH_JSON_BINDINGS=none"
    ];

    CXXFLAGS = [
      "-DFMT_USE_INTERNAL=TRUE"
    ];

    buildInputs = [
      boost
      gtest
      fmt_9
      rapidjson

      expected-lite
      optional-lite
      string-view-lite
      variant-lite
    ];
  };
}
