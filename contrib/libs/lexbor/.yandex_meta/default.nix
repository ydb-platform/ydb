self: super: with self; {
  lexbor = stdenv.mkDerivation rec {
    name = "lexbor";
    version = "2.5.0";

    src = fetchFromGitHub {
      owner = "lexbor";
      repo = "lexbor";
      rev = "v${version}";
      hash = "sha256-QmD8p6dySLEeHjCmDSTplXwsy2Z7yHKYlXmPCKwaBfU=";
    };

    nativeBuildInputs = [ cmake ];

    cmakeFlags = [
      "-DLEXBOR_BUILD_TESTS=OFF"
      "-DLEXBOR_BUILD_EXAMPLES=OFF"

      # Build only shared version of lexbor to avoid liblexbor_static.a manipulation
      "-DLEXBOR_BUILD_SHARED=ON"
      "-DLEXBOR_BUILD_STATIC=OFF"

      # Build all lexbor modules separately, yet install only html submodule and its dependencies
      # Unfortunately, overriding LEXBOR_MODULES property does not work.
      "-DLEXBOR_BUILD_SEPARATELY=ON"

      # Override -pipe option which is enabled by default
      # Using this option breaks traces, see DTCC-589.
      # lexbor project ignores standard CMAKE_CXX_FLAGS for unknown reason.
      "-DLEXBOR_C_FLAGS=-Wall"
    ];
  };
}

