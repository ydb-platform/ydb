self: super: with self; {
  protobuf_mutator = stdenv.mkDerivation rec {
    pname = "protobuf_mutator";
    version = "1.5";

    src = fetchFromGitHub {
      owner = "google";
      repo = "libprotobuf-mutator";
      rev = "v${version}";
      hash = "sha256-0iohTyYcV76OOaq9zBpQ6FB+ipxlK/fm/4N7Gp8vnwA=";
    };

    buildInputs = [ pkgs.protobuf ];

    nativeBuildInputs = [ cmake ];

    cmakeFlags = [
      "-DLIB_PROTO_MUTATOR_TESTING=OFF"
    ];
  };
}
