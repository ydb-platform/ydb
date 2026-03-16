self: super: with self; with python310.pkgs; {
  horovod = buildPythonPackage rec {
    pname = "horovod";
    version = "0.16.4";

    src = fetchPypi {
      inherit pname version;
      sha256 = "04jqikkgfggi7h6bv7zbz3vl0hs9jmywwy7f6ykdszggvzqfj7c7";
    };

    ghsrc = fetchFromGitHub {
      owner = "horovod";
      repo = "horovod";
      rev = "v${version}";
      sha256 = "1gnmx2vw0x5irq6sl3n14mn3q3irmkkw55dc79ij0dlxnnqnwjm5";
    };

    postUnpack = ''
      cp ${ghsrc}/horovod/common/wire/message.fbs $sourceRoot/horovod/common/wire/
    '';

    # buildInputs = [ tensorflow ];

    HOROVOD_WITHOUT_PYTORCH = true;
    HOROVOD_WITHOUT_MXNET = true;
  };
}
