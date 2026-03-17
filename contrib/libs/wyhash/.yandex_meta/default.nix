self: super: with self; {
  yamaker-wyhash = stdenv.mkDerivation rec {
    name = "wyhash";
    version = "2024-06-07";
    revision = "46cebe9dc4e51f94d0dca287733bc5a94f76a10d";

    src = fetchFromGitHub {
      owner = "wangyi-fudan";
      repo = "wyhash";
      rev = "${revision}";

      hash = "sha256-5sJi38T+pRjt+2lZ+55bRoKFQtYIUgyx69Vgxf2jvdc=";
    };
  };
}
